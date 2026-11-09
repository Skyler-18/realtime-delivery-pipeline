"""Graceful shutdown: turn Ctrl+C / `docker stop` (SIGTERM) into a flag the main loop checks.

Without this, SIGTERM kills Python immediately: buffered producer messages are lost and
consumers don't commit or leave the group cleanly (causing a slow rebalance).
"""

import logging
import signal
import threading

log = logging.getLogger(__name__)


def install_shutdown_handler() -> threading.Event:
    stop = threading.Event()

    def _handle(signum: int, _frame: object) -> None:
        log.info("Received signal %s, shutting down gracefully", signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    return stop
