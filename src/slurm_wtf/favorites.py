"""Persistent favorites, identified by cluster, partition, and exact account name."""

import fcntl
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile


def favorites_path(demo=False):
    directory = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    return directory / "slurm-wtf" / ("demo-favorites.json" if demo else "favorites.json")


def read_favorites(path):
    try:
        content = path.read_text()
    except FileNotFoundError:
        return set()
    data = json.loads(content)
    if not isinstance(data, list) or any(
        not isinstance(key, list) or len(key) != 3 or not all(isinstance(v, str) for v in key)
        for key in data
    ):
        raise ValueError(f"Invalid favorites in {path}")
    return {tuple(key) for key in data}


def set_favorite(path, key, enabled):
    """Merge changes with other sessions and replace the settings file atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        favorites = read_favorites(path)
        if enabled:
            favorites.add(key)
        else:
            favorites.discard(key)
        with NamedTemporaryFile(
            mode="w", dir=path.parent, prefix=".favorites-", delete=False
        ) as out:
            temporary = Path(out.name)
            try:
                out.write(json.dumps(sorted(favorites), indent=2) + "\n")
                out.flush()
                os.fsync(out.fileno())
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
    return favorites
