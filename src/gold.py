import sys
from pathlib import Path
import pandas as pd

if __name__ == "__main__" and __package__ is None:
    sys.path.append(str(Path(__file__).resolve().parent.parent))
try:
    from src.config import SILVER_PATH, GOLD_PATH
    from src.logger import get_logger
except ImportError:
    from config import SILVER_PATH, GOLD_PATH
    from logger import get_logger

logger = get_logger()

def create_gold():

    logger.info("Iniciando camada Gold")

    silver_files = list(Path(SILVER_PATH).rglob("*.parquet"))
    if not silver_files:
        logger.warning("Nenhum arquivo na Silver para agregar")
        return

    frames = []
    required_cols = {"country", "state", "brewery_type"}

    def infer_partitions(path: Path):
        # path may contain segments like country=Foo and state=Bar
        from urllib.parse import unquote
        country = None
        state = None
        for segment in path.parts:
            if segment.startswith("country="):
                country = unquote(segment.split("=", 1)[1])
            elif segment.startswith("state="):
                state = unquote(segment.split("=", 1)[1])
        return country, state

    read_errors = 0
    missing_cols = 0

    for fp in silver_files:
        try:
            part = pd.read_parquet(fp, engine="pyarrow")
        except Exception as exc:
            logger.warning("falha lendo %s: %s -- pulando", fp, exc)
            read_errors += 1
            continue
        missing = required_cols - set(part.columns)
        if missing:
            country, state = infer_partitions(fp)
            if "country" in missing and country is not None:
                part["country"] = country
            if "state" in missing and state is not None:
                part["state"] = state
            missing = required_cols - set(part.columns)
        if missing:
            logger.warning("arquivo %s sem colunas necessárias %s -- pulando", fp, missing)
            missing_cols += 1
            continue
        frames.append(part)

    if not frames:
        logger.warning("nenhum dado válido lido da Silver; gold não será gerada")
        return

    df = pd.concat(frames, ignore_index=True)

    agg = (
        df.groupby(["country", "state", "brewery_type"])
        .size()
        .reset_index(name="total_breweries")
    )

    # emit metrics before writing
    def emit_metrics(agg_df, total_silver_files, skipped, corrupted):
        total_rows = len(agg_df)
        partitions = agg_df[["country", "state"]].drop_duplicates().shape[0]
        recent_date = agg_df["ingestion_date"].max() if "ingestion_date" in agg_df.columns else None

        logger.info("METRIC gold_rows=%d partitions=%d recent_ingest=%s",
                    total_rows, partitions, recent_date)
        logger.info("METRIC silver_files=%d skipped=%d corrupted=%d",
                    total_silver_files, skipped, corrupted)

    emit_metrics(agg, len(silver_files), skipped=missing_cols, corrupted=read_errors)

    GOLD_PATH.mkdir(parents=True, exist_ok=True)

    agg.to_parquet(
        GOLD_PATH / "breweries_aggregated.parquet",
        index=False,
    )
if __name__ == "__main__":
   create_gold()

