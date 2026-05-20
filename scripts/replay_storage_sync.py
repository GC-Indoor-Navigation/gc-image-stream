from __future__ import annotations

import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.storage_sync_replay.cli import main  # noqa: E402


if __name__ == "__main__":
    main()
