"""Process-wide logging setup. (Switched to structured JSON logs in Phase 4.)"""

import logging
import os


def setup_logging(service: str) -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format=f"%(asctime)s %(levelname)-7s [{service}] %(name)s: %(message)s",
    )
