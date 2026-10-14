"""Coloca o CSV real do PaySim em data/paysim/ e confere se ele tem o formato esperado.

O dataset (https://www.kaggle.com/datasets/ealaxi/paysim1) exige login no Kaggle, por isso
não está no repositório. Três jeitos de trazê-lo:

  # 1. com a CLI do Kaggle (precisa de ~/.kaggle/kaggle.json)
  python -m scripts.dados.preparar_paysim --kaggle

  # 2. a partir do zip baixado pelo navegador (botão Download da página do dataset)
  python -m scripts.dados.preparar_paysim --zip ~/Downloads/archive.zip

  # 3. a partir do CSV já extraído
  python -m scripts.dados.preparar_paysim --csv ~/Downloads/PS_20174392719_1491204439457_log.csv

Depois disso basta subir o backend: o seed lê data/paysim/ (PAYSIM_DATA_DIR) sozinho.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import subprocess
import zipfile
from collections import Counter
from pathlib import Path

from scripts.comum.paysim import ARQ_KAGGLE, COLUNAS

RAIZ = Path(__file__).resolve().parents[2]


def _n(v: int) -> str:
    return f"{v:,}".replace(",", ".")


def conferir(arquivo: Path) -> None:
    """Confere o cabeçalho e resume o arquivo (linhas, tipos, fraudes, steps)."""
    with open(arquivo, newline="", encoding="utf-8-sig") as f:
        r = csv.reader(f)
        cab = [c.strip() for c in next(r)]
        faltando = [c for c in COLUNAS if c not in cab]
        if faltando:
            raise SystemExit(f"{arquivo}: colunas ausentes {faltando}; esperado {','.join(COLUNAS)}")
        i_tipo, i_fraude, i_step = cab.index("type"), cab.index("isFraud"), cab.index("step")
        tipos, fraudes, linhas, step_ant, fora_de_ordem = Counter(), 0, 0, 0, 0
        for linha in r:
            linhas += 1
            tipos[linha[i_tipo]] += 1
            fraudes += linha[i_fraude] == "1"
            step = int(linha[i_step])
            fora_de_ordem += step < step_ant
            step_ant = step
    print(f"{arquivo}: {_n(linhas)} linhas, {_n(fraudes)} fraudes, último step {step_ant}")
    print("  tipos: " + ", ".join(f"{t} {_n(n)}" for t, n in tipos.most_common()))
    if fora_de_ordem:
        print(f"  ATENÇÃO: {fora_de_ordem} linhas fora da ordem de step; o seed e a carga usam a ordem do arquivo")
    if linhas != 6362620:
        print("  (o arquivo original do Kaggle tem 6.362.620 linhas)")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--kaggle", action="store_true", help="baixa com a CLI do Kaggle")
    g.add_argument("--zip", type=Path, help="zip baixado da página do dataset")
    g.add_argument("--csv", type=Path, help="CSV já extraído")
    p.add_argument("--destino", type=Path, default=RAIZ / "data" / "paysim")
    a = p.parse_args()
    a.destino.mkdir(parents=True, exist_ok=True)
    alvo = a.destino / ARQ_KAGGLE

    if a.kaggle:
        cmd = ["kaggle", "datasets", "download", "-d", "ealaxi/paysim1", "-p", str(a.destino), "--unzip"]
        print("$ " + " ".join(cmd))
        try:
            subprocess.run(cmd, check=True)
        except FileNotFoundError:
            raise SystemExit("CLI do Kaggle não encontrada: pip install kaggle e configure ~/.kaggle/kaggle.json")
    elif a.zip:
        with zipfile.ZipFile(a.zip) as z:
            nomes = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if len(nomes) != 1:
                raise SystemExit(f"esperado um CSV dentro de {a.zip}, achados: {nomes}")
            print(f"extraindo {nomes[0]} para {alvo}")
            with z.open(nomes[0]) as src, open(alvo, "wb") as dst:
                shutil.copyfileobj(src, dst, 1 << 20)
    elif a.csv:
        if a.csv.resolve() != alvo.resolve():
            print(f"copiando {a.csv} para {alvo}")
            shutil.copyfile(a.csv, alvo)

    csvs = sorted(a.destino.glob("*.csv"))
    if not csvs:
        raise SystemExit(f"nenhum CSV em {a.destino}. Use --kaggle, --zip ou --csv (veja --help).")
    if len(csvs) > 1:
        print(f"ATENÇÃO: há {len(csvs)} CSVs em {a.destino}; o seed usa {ARQ_KAGGLE} ou o definido em PAYSIM_FILE")
    for c in csvs:
        conferir(c)
    if (a.destino / "_SINTETICO").exists():
        print(f"ATENÇÃO: {a.destino} tem o marcador _SINTETICO; apague-o se esta pasta agora tem o dataset real")
    print("\nPronto. Garanta PAYSIM_DATA_DIR=./data/paysim no .env (é o padrão) e suba o backend.")


if __name__ == "__main__":
    main()
