import json
import glob
import pandas as pd


with open("data/bronze/2026-02-27/breweries_raw.json") as f:
    data = json.load(f)
print("Numero de linhas no source:", len(data))

missouri = [r for r in data if r.get("state") == "Missouri"]
print("Numero de linhas em Missouri:", len(missouri))


# Volume Bronze
bronze_files = glob.glob("data/bronze/*/breweries_raw.json")
bronze_count = 0

for f in bronze_files:
    with open(f) as file:
        bronze_count += len(json.load(file))

print(f"Total de arquivos ingeridos: {len(bronze_files)}")
print(f"Total de linhas lidas: {bronze_count}")

bronze_data = []

for f in bronze_files:
    with open(f) as file:
        bronze_data.extend(json.load(file))

df_bronze = pd.DataFrame(bronze_data)
df_bronze.columns = [c.lower() for c in df_bronze.columns]

audit = (
    df_bronze
    .assign(is_duplicate=df_bronze.duplicated(subset=["id"], keep="first"))
    .groupby("state")["is_duplicate"]
    .sum()
    .sort_values(ascending=False)
)

print("Duplicados por estado:", audit)


# Padronizar colunas
df_bronze.columns = [c.lower() for c in df_bronze.columns]

if "state_province" in df_bronze.columns:
    df_bronze["state"] = df_bronze["state_province"]
    df_bronze = df_bronze.drop(columns=["state_province"])

duplicados = df_bronze[df_bronze.duplicated(subset=["id"], keep=False)]

duplicados_ordenados = (
    duplicados
    .sort_values(by=["id", "state"])
)

print("Duplicados encontrados:", duplicados_ordenados[[
    "id",
    "name",
    "country",
    "state",
    "brewery_type"
]])

