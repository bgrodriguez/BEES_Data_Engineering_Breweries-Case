import pandas as pd
import pyarrow.parquet as pq
import glob
import json

def audit():

    # --- Testando leitura dos arquivos Parquet na Silver para validar integridade física ---

    files = glob.glob("data/silver/**/*.parquet", recursive=True)

    for f in files:
        try:
            pq.read_table(f)
        except Exception as e:
            print(f"Arquivo corrompido: {f}") # <--- Mostra qual arquivo está corrompido --- IGNORE ---
            print(e)

    print("✔ Validação física concluída.")

    # Volume Bronze
    bronze_files = glob.glob("data/bronze/*/breweries_raw.json")
    bronze_count = 0

    for f in bronze_files:
        with open(f) as file:
            bronze_count += len(json.load(file))

    # Volume Silver
    df = pd.read_parquet("data/silver")

    print(f"Bronze: {bronze_count}")
    print(f"Silver: {len(df)}")

    if bronze_count != len(df):
        print("⚠ Diferença de volume detectada")
    else:
        print("✔ Volume consistente")

    print("Duplicados:", df.duplicated(subset=["id"]).sum())

if __name__ == "__main__":
    audit()


import duckdb

con = duckdb.connect()

df = con.execute("""
SELECT *
FROM read_parquet('data/silver/**/*.parquet')
LIMIT 10
""").df()

print(df)
    