import sys
import requests
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if __name__ == "__main__" and __package__ is None:
    sys.path.append(str(Path(__file__).resolve().parent.parent))
try:
    from src.config import BASE_URL, BRONZE_PATH
    from src.logger import get_logger
except ImportError:
    from config import BASE_URL, BRONZE_PATH
    from logger import get_logger

logger = get_logger()

# --- HTTP Session com Retry ---

def create_session() -> requests.Session:
    session = requests.Session()

    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session

# --- API Consumption ---

def fetch_breweries(session: requests.Session, page: int, per_page: int = 200) -> List[Dict]:
    try:
        response = session.get(
            BASE_URL,
            params={"page": page, "per_page": per_page},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"[API ERROR] Page {page}: {e}")
        raise

# --- Bronze Ingestion ---

def ingest_bronze() -> None:
    logger.info("Iniciando ingestão Bronze")

    today = datetime.utcnow().strftime("%Y-%m-%d")
    output_path = BRONZE_PATH / today
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / "breweries_raw.json"

    # Idempotência — evita sobrescrever ingestão do mesmo dia
    if file_path.exists():
        logger.warning(f"Ingestão já realizada para {today}. Pulando execução.")
        return

    session = create_session()

    all_data: List[Dict] = []
    page = 1

    while True:
        data = fetch_breweries(session, page)

        if not data:
            logger.info("Fim da paginação.")
            break

        all_data.extend(data)
        logger.info(f"Página {page} coletada com {len(data)} registros.")
        page += 1

    if not all_data:
        logger.warning("Nenhum dado retornado da API.")
        return

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)

        logger.info(
            f"Ingestão concluída com sucesso. Total registros: {len(all_data)}"
        )

    except Exception as e:
        logger.error(f"[FILE WRITE ERROR] {e}")
        raise

if __name__ == "__main__":
    ingest_bronze()
