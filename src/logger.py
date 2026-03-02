import logging
import sys
from pathlib import Path

# support running as script
if __name__ == "__main__" and __package__ is None:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

try:
    from src.config import LOG_PATH
except ImportError:
    from config import LOG_PATH

def get_logger():
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("pipeline")