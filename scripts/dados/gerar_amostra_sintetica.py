"""Gera uma amostra SINTÉTICA no mesmo formato do CSV do dataset PaySim.

Serve para validar o pipeline sem o dataset real (que exige login no Kaggle). As colunas, a
ordem por step e o formato dos valores seguem o original; a distribuição de tipos, a
proporção de fraudes, os comerciantes ("M...") só em PAYMENT e a reutilização das contas de
destino imitam as do PaySim. NÃO usar nos experimentos do TCC.

A pasta recebe um arquivo _SINTETICO, que o seed detecta e registra em seed_info.synthetic.

Uso:
  python -m scripts.dados.gerar_amostra_sintetica --linhas 200000 --saida data/paysim-sintetico
  echo "PAYSIM_DATA_DIR=./data/paysim-sintetico" >> .env
"""

from __future__ import annotations

import argparse
import csv
import math
import random
from pathlib import Path

from scripts.comum.paysim import COLUNAS

# Proporções do PaySim original (6.362.620 linhas).
TIPOS = [("CASH_OUT", 0.3517), ("PAYMENT", 0.3381), ("CASH_IN", 0.2199), ("TRANSFER", 0.0838), ("DEBIT", 0.0065)]
# Mediana aproximada do valor por tipo.
MEDIANAS = {"CASH_OUT": 147000, "PAYMENT": 9500, "CASH_IN": 143000, "TRANSFER": 486000, "DEBIT": 3000}
STEPS = 743
TAXA_FRAUDE = 0.0013


def escolher(rnd: random.Random, pares):
    x, acc = rnd.random(), 0.0
    for v, p in pares:
        acc += p
        if x <= acc:
            return v
    return pares[-1][0]


def gerar(n: int, saida: Path, semente: int) -> Path:
    rnd = random.Random(semente)
    saida.mkdir(parents=True, exist_ok=True)
    usados: set[str] = set()

    def nova_conta(prefixo: str) -> str:
        while True:
            c = f"{prefixo}{rnd.randint(10**8, 2 * 10**9)}"
            if c not in usados:
                usados.add(c)
                return c

    # Contas de destino de clientes são reaproveitadas (no PaySim ~2,7 milhões de destinos para
    # 6,3 milhões de linhas); uma parte pequena é muito usada.
    destinos = [nova_conta("C") for _ in range(max(50, int(n * 0.35)))]
    quentes = destinos[: max(5, len(destinos) // 200)]
    saldos: dict[str, float] = {}
    origens_recentes: list[str] = []

    arquivo = saida / "paysim_sintetico.csv"
    with open(arquivo, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLUNAS)
        for i in range(n):
            step = 1 + (i * STEPS) // n
            tipo = escolher(rnd, TIPOS)
            valor = round(min(9.2e7, rnd.lognormvariate(math.log(MEDIANAS[tipo]), 1.0)), 2)

            # Quase toda origem é uma conta nova; ~1% repete uma origem recente.
            if origens_recentes and rnd.random() < 0.01:
                orig = rnd.choice(origens_recentes)
            else:
                orig = nova_conta("C")
                saldos[orig] = 0.0 if rnd.random() < 0.33 else round(rnd.lognormvariate(math.log(50000), 1.5), 2)
                origens_recentes.append(orig)
                if len(origens_recentes) > 1000:
                    origens_recentes.pop(0)

            if tipo == "PAYMENT":
                dest = nova_conta("M")
                saldos[dest] = 0.0
            else:
                dest = rnd.choice(quentes) if rnd.random() < 0.1 else rnd.choice(destinos)
                saldos.setdefault(dest, 0.0 if rnd.random() < 0.4 else round(rnd.lognormvariate(math.log(800000), 1.2), 2))

            antigo_o, antigo_d = saldos[orig], saldos[dest]
            if tipo == "CASH_IN":
                novo_o = antigo_o + valor
                novo_d = max(0.0, antigo_d - valor)
            else:
                novo_o = max(0.0, antigo_o - valor)
                novo_d = antigo_d + valor if dest.startswith("C") else 0.0
            saldos[orig], saldos[dest] = round(novo_o, 2), round(novo_d, 2)

            fraude = tipo in ("TRANSFER", "CASH_OUT") and rnd.random() < TAXA_FRAUDE * 4
            sinalizada = fraude and tipo == "TRANSFER" and valor > 200000 and rnd.random() < 0.01
            w.writerow([step, tipo, f"{valor:.2f}", orig, f"{antigo_o:.2f}", f"{saldos[orig]:.2f}",
                        dest, f"{antigo_d:.2f}", f"{saldos[dest]:.2f}", int(fraude), int(sinalizada)])
    (saida / "_SINTETICO").write_text("Amostra sintética gerada por scripts/dados/gerar_amostra_sintetica.py. "
                                      "Não usar nos experimentos.\n")
    return arquivo


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--linhas", type=int, default=200000)
    p.add_argument("--saida", default="data/paysim-sintetico")
    p.add_argument("--semente", type=int, default=42)
    a = p.parse_args()
    arquivo = gerar(a.linhas, Path(a.saida), a.semente)
    print(f"{a.linhas} linhas sintéticas em {arquivo}")


if __name__ == "__main__":
    main()
