"""Tiny logger module used across the project.

The module keeps logging extremely small and friendly: messages are written
both to stdout and to a file named ``arb.log`` in the project directory.
"""

from __future__ import annotations

import logging
from pathlib import Path

LOG_FILE = Path('arb.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger('arb')


def log_error(message: str) -> None:
    """Log an error message.

    Parameters
    ----------
    message:
        The text to log.
    """

    logger.error(message)
