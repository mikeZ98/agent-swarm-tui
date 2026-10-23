"""Textual terminal UI for orchestrating and observing an asyncio multi-agent swarm."""

import logging

# Defined before the imports below: submodules read it during package import.
__version__ = "0.1.0"

# Library convention: no output unless the application configures logging.
# Without this, Python's last-resort handler would print warnings to stderr
# and corrupt the terminal UI (``main`` installs a file/devtools handler).
logging.getLogger(__name__).addHandler(logging.NullHandler())

from agent_swarm_tui.app import SwarmApp, main  # noqa: E402

__all__ = ["SwarmApp", "__version__", "main"]
