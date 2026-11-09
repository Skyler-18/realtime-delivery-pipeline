"""Run: uv run python -m delivery_pipeline.live_map   -> http://localhost:8000"""

import uvicorn

from delivery_pipeline.config import LiveMapSettings
from delivery_pipeline.live_map.app import create_app
from delivery_pipeline.logging_setup import setup_logging


def main() -> None:
    setup_logging("live-map")
    settings = LiveMapSettings()
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port, log_config=None)


if __name__ == "__main__":
    main()
