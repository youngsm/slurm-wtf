"""User-configured filler accounts scoped by Slurm's cluster name."""

import fcntl
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile


def settings_path():
    root = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    return root / "slurm-wtf" / "settings.json"


def read_settings(path):
    try:
        data = json.loads(path.read_text())
    except FileNotFoundError:
        return {}
    if not isinstance(data, dict) or any(
        not isinstance(users, list) or not all(isinstance(user, str) for user in users)
        for users in data.values()
    ):
        raise ValueError("Invalid filler-user settings in " + str(path))
    return data


def read_filler_users(cluster):
    return tuple(read_settings(settings_path()).get(cluster, ()))


def save_filler_users(cluster, users):
    path = settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        data = read_settings(path)
        data[cluster] = sorted(set(users))
        with NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as out:
            temporary = Path(out.name)
            try:
                json.dump(data, out, indent=2)
                out.write("\n")
                out.close()
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
    return path
