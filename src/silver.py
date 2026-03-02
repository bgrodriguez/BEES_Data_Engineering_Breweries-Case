import sys
from pathlib import Path
import pandas as pd

if __name__ == "__main__" and __package__ is None:
    sys.path.append(str(Path(__file__).resolve().parent.parent))
try:
    from src.config import BRONZE_PATH, SILVER_PATH
    from src.logger import get_logger
except ImportError:  
    from config import BRONZE_PATH, SILVER_PATH
    from logger import get_logger

logger = get_logger()

def transform_silver():
    logger.info("Iniciando transformação Silver")

    bronze_folders = sorted([f for f in BRONZE_PATH.glob("*") if f.is_dir()]) # <--- Busca apenas pastas de data --- IGNORE ---

    all_dfs = []

    # collect data from all bronze folders first
    for folder in bronze_folders:
        ingestion_date = folder.name
        file_path = folder / "breweries_raw.json"

        if not file_path.exists():
            logger.warning(f"Arquivo não encontrado: {file_path}")
            continue
        
        logger.info(f"Processando Bronze: {file_path}")

        df = pd.read_json(file_path)

        # === Padronização de nomes ===
        df.columns = [c.lower() for c in df.columns]

        # === Tratamento de Nulos ===
        df["country"] = df["country"].fillna("unknown")
        df["state"] = df["state"].fillna("unknown")
        df["brewery_type"] = df["brewery_type"].fillna("unknown")

        # === Deduplicação ===
        df = df.drop_duplicates(subset=["id"])

        # == Tipagem explícita (EVITA dictionary encoding conflitante) ==
        df = df.astype({
            "country": "string",
            "state": "string",
            "brewery_type": "string"
        })

        # === Padronização ===
        df.columns = [c.lower() for c in df.columns]
        df["ingestion_date"] = ingestion_date
        
        all_dfs.append(df)

    if not all_dfs:
        logger.warning("Nenhum dataframe válido para processar.")
        return

    final_df = pd.concat(all_dfs, ignore_index=True)

    # remove duplicates across ingestion dates
    final_df = final_df.drop_duplicates(subset=["id"])

    # === Validação de dados antes da escrita ===
    # Remove linhas com valores NaN em colunas críticas ou strings muito longas
    initial_rows = len(final_df)
    
    # Drop rows com NaN em colunas essenciais
    final_df = final_df.dropna(subset=["latitude", "longitude"])
    
    # Limpar strings muito longas (pode causar corrupção BYTE_ARRAY)
    string_cols = final_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        if col in final_df.columns:
            # Truncar strings muito longas (>65KB)
            final_df[col] = final_df[col].apply(
                lambda x: str(x)[:65000] if isinstance(x, str) and len(str(x)) > 65000 else x
            )
    
    dropped_rows = initial_rows - len(final_df)
    if dropped_rows > 0:
        logger.info("Limpeza de dados: %d linhas removidas por NaN ou dados inválidos", dropped_rows)

    # === Limpeza da Silver antes de regravar ===
    # === (evita conflito de schema entre execuções) ===
    if SILVER_PATH.exists():
        logger.info("Limpando camada Silver anterior para evitar conflito de schema")
        import shutil
        shutil.rmtree(SILVER_PATH)

    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    # === Escrita particionada ===
    final_df.to_parquet(
        SILVER_PATH,
        engine="pyarrow",
        partition_cols=["country", "state"],
        index=False,
        compression="snappy",
        use_dictionary=False  # FORÇA desabilitar dictionary encoding
    )

    logger.info("Silver escrita com sucesso, validando arquivos..." )

    # pós-validação: diagnosticar a quarentena de arquivos corrompidos
    import pyarrow.parquet as pq
    import shutil
    from datetime import datetime
    
    corrupted_dir = SILVER_PATH.parent / "corrupted_files"
    corrupted_dir.mkdir(parents=True, exist_ok=True)
    
    corrupted = 0
    for fp in SILVER_PATH.rglob("*.parquet"):
        try:
            pq.read_table(fp)
        except Exception as e:
            # Quarentena: mover para diagnóstico ao invés de deletar
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest_name = f"{fp.stem}_{timestamp}.parquet"
            dest_path = corrupted_dir / dest_name
            logger.error("Arquivo corrompido encontrado: %s | Erro: %s | Movendo para quarentena: %s", 
                        fp, str(e)[:100], dest_path)
            try:
                shutil.move(str(fp), str(dest_path))
                logger.info("Arquivo movido para quarentena: %s", dest_path)
            except Exception as move_err:
                logger.error("Falha ao mover arquivo: %s", move_err)
                fp.unlink()  # fallback: deletar se não conseguir mover
            corrupted += 1
    
    if corrupted:
        logger.warning("Quarentenados %d arquivos corruptos em: %s", corrupted, corrupted_dir)
        logger.info("Para diagnosticar, execute: python src/recover_corrupted.py '<caminho_para_arquivo>'")

if __name__ == "__main__":
    transform_silver()
