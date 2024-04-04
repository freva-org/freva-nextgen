"""Print command line argument options."""

import logging
import sys

from freva_databrowser.cli_utils import Completer
from freva_databrowser.utils import logger

logger.set_level(logging.CRITICAL)


try:
    comp = Completer.parse_choices(
        [arg.strip() for arg in sys.argv[1:] if arg.strip()]
    )
    comp.formated_print()
except (ValueError, KeyboardInterrupt):
    pass
