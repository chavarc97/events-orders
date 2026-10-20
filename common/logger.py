import logging
import os
import sys

LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_FILE = os.path.join(LOG_DIR, "events.log")

os.makedirs(LOG_DIR, exist_ok=True)

_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(_formatter)

_file = logging.FileHandler(LOG_FILE)
_file.setFormatter(_formatter)

logger = logging.getLogger("events-orders")
logger.setLevel(logging.INFO)
if not logger.handlers:  # avoid duplicate handlers if imported twice
    logger.addHandler(_console)
    logger.addHandler(_file)
