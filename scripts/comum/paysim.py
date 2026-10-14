"""Leitura do dataset PaySim e montagem das operações de negócio reproduzidas pela API.

A regra de corte é a mesma do seed (cmd/seed/main.go): cada linha do CSV recebe o
identificador T + posição (T000000001 é a primeira), consideram-se as primeiras
`transactions_total` linhas e as primeiras `transactions_seeded` delas vão para a carga
inicial. As demais formam o conjunto de reprodução, usado como carga transacional. Os dois
números ficam gravados em seed_info, então os scripts não precisam recontar o arquivo.

O CSV tem 6,3 milhões de linhas, por isso é lido em fluxo, sem carregar tudo na memória.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, replace
from pathlib import Path

import psycopg

COLUNAS = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
           "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"]
ARQ_KAGGLE = "PS_20174392719_1491204439457_log.csv"


def _do_env(chave: str) -> str | None:
    """Variável do processo ou, se ausente, do .env da raiz (o mesmo que o Compose lê)."""
    valor = os.environ.get(chave)
    env = Path(__file__).resolve().parents[2] / ".env"
    if not valor and env.exists():
        for linha in env.read_text().splitlines():
            k, _, v = linha.partition("=")
            if k.strip() == chave and v.strip():
                valor = v.strip().strip('"').strip("'")
    return valor or None


def pasta_dados() -> Path:
    """Mesma pasta montada no contêiner do seed: PAYSIM_DATA_DIR do ambiente ou do .env."""
    raiz = Path(__file__).resolve().parents[2]
    return (raiz / (_do_env("PAYSIM_DATA_DIR") or "./data/paysim")).resolve()


def id_transacao(n: int) -> str:
    """Identificador da linha n do CSV (1 = primeira). Precisa coincidir com o seed."""
    return f"T{n:09d}"


@dataclass(frozen=True)
class Operacao:
    """Uma linha do PaySim, reproduzida como POST /transactions."""
    transaction_id: str
    step: int
    type: str
    amount: str
    name_orig: str
    name_dest: str
    old_balance_orig: str
    old_balance_dest: str
    is_fraud: bool
    is_flagged_fraud: bool

    def corpo(self) -> dict:
        return {"transaction_id": self.transaction_id, "step": self.step, "type": self.type,
                "amount": self.amount, "name_orig": self.name_orig, "name_dest": self.name_dest,
                "old_balance_orig": self.old_balance_orig, "old_balance_dest": self.old_balance_dest,
                "is_fraud": self.is_fraud, "is_flagged_fraud": self.is_flagged_fraud}

    def clone(self, rodada: int) -> "Operacao":
        """Cópia com transaction_id novo, para reutilizar o conjunto em cargas longas. As
        contas são as mesmas, então a cópia movimenta contas que já existem."""
        return replace(self, transaction_id=f"{self.transaction_id}-r{rodada}")


def info_seed(dsn: str) -> dict:
    """Parâmetros da carga inicial gravados pelo seed."""
    with psycopg.connect(dsn) as c:
        r = c.execute("""SELECT seed_fraction, data_file, transactions_total, transactions_seeded,
                                accounts_seeded, synthetic FROM seed_info""").fetchone()
    if r is None:
        raise SystemExit("seed_info vazio: a carga inicial não rodou")
    info = dict(zip(("fracao", "arquivo", "total", "carregadas", "contas", "sintetico"), r))
    if info["sintetico"]:
        print("ATENÇÃO: o banco foi carregado com a amostra SINTÉTICA, não com o dataset PaySim real")
    return info


class FluxoOperacoes:
    """Itera o conjunto de reprodução (linhas carregadas+1 até total). Com repetir=True,
    recomeça com clones de transaction_id novo quando o conjunto acaba."""

    def __init__(self, info: dict, pasta: Path | None = None, repetir: bool = True, inicio_rodada: int = 0):
        self.caminho = (pasta or pasta_dados()) / info["arquivo"]
        if not self.caminho.exists():
            raise SystemExit(f"{self.caminho} não encontrado. O seed usou {info['arquivo']}; confira "
                             f"PAYSIM_DATA_DIR (README, seção Dados).")
        self.inicio, self.fim = info["carregadas"], info["total"]
        if self.fim <= self.inicio:
            raise SystemExit("não sobrou nenhuma linha para a carga: reduza SEED_FRACTION")
        self.repetir, self.rodada = repetir, inicio_rodada
        self._abrir()

    def __len__(self) -> int:
        return self.fim - self.inicio

    def _abrir(self) -> None:
        self._f = open(self.caminho, newline="", encoding="utf-8-sig")
        self._r = csv.reader(self._f)
        cab = next(self._r)
        self._idx = {c.strip(): i for i, c in enumerate(cab)}
        faltando = [c for c in COLUNAS if c not in self._idx]
        if faltando:
            raise SystemExit(f"{self.caminho}: colunas ausentes {faltando}")
        for _ in range(self.inicio):  # pula a parcela da carga inicial
            next(self._r)
        self._n = self.inicio

    def __iter__(self):
        return self

    def __next__(self) -> Operacao:
        if self._n >= self.fim:
            self._f.close()
            if not self.repetir:
                raise StopIteration
            self.rodada += 1
            self._abrir()
        linha = next(self._r)
        self._n += 1
        c = lambda nome: linha[self._idx[nome]]
        op = Operacao(id_transacao(self._n), int(c("step")), c("type"), c("amount"), c("nameOrig"), c("nameDest"),
                      c("oldbalanceOrg") or "0", c("oldbalanceDest") or "0", c("isFraud") == "1",
                      c("isFlaggedFraud") == "1")
        return op if self.rodada == 0 else op.clone(self.rodada)
