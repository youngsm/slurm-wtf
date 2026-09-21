"""Best-effort release notifications; never install or change the running version."""

import json
import os
import re
import ssl
import threading
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.request import Request, urlopen

from . import __version__

INDEX_URL = "https://pypi.org/pypi/slurm-wtf/json"
CURRENT_CACHE_SECONDS = 60 * 60
NOTICE_CACHE_SECONDS = 24 * 60 * 60
FAILURE_CACHE_SECONDS = 5 * 60
TIMEOUT = 2
CA_BUNDLES = (
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/pki/tls/cert.pem",
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/ssl/cert.pem",
)


def stable_version(value):
    """This project publishes stable major.minor.patch versions."""
    if isinstance(value, str) and re.fullmatch(r"\d+\.\d+\.\d+", value):
        return tuple(map(int, value.split(".")))
    return None


def cache_path():
    root = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
    return root / "slurm-wtf" / "update-check.json"


def tls_context():
    """Use the runtime defaults, falling back to common host CA bundles."""
    defaults = ssl.get_default_verify_paths()
    if defaults.cafile:
        return ssl.create_default_context()
    bundle = next((path for path in CA_BUNDLES if Path(path).is_file()), None)
    return ssl.create_default_context(cafile=bundle)


def cache_seconds(latest, success):
    if not success:
        return FAILURE_CACHE_SECONDS
    current, available = stable_version(__version__), stable_version(latest)
    return (
        NOTICE_CACHE_SECONDS
        if current and available and available > current
        else CURRENT_CACHE_SECONDS
    )


def check_release(force=False):
    """Return (latest, error), retaining a known release if an offline check fails."""
    path = cache_path()
    latest, checked, success = None, 0, False
    try:
        data = json.loads(path.read_text())
        if isinstance(data, dict):
            latest = data.get("latest")
            checked = float(data.get("checked", 0))
            success = bool(data.get("success", latest is not None))
    except (OSError, ValueError, TypeError):
        pass
    if not stable_version(latest):
        latest = None
    now = time.time()
    if not force and 0 <= now - checked < cache_seconds(latest, success):
        return latest, ""
    error = ""
    try:
        request = Request(INDEX_URL, headers={"User-Agent": "slurm-wtf/" + __version__})
        with urlopen(request, timeout=TIMEOUT, context=tls_context()) as response:
            data = json.load(response)
        # Ignore prereleases, empty releases, and releases with only yanked files.
        versions = [
            version
            for version, artifacts in data["releases"].items()
            if stable_version(version) and any(not item.get("yanked", False) for item in artifacts)
        ]
        latest = max(versions, key=stable_version)
        success = True
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        error = "Could not check for updates: " + str(exc)
        success = False
    # Cache failed attempts too, so offline clusters do not retry every launch.
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as out:
            temporary = Path(out.name)
            try:
                json.dump({"latest": latest, "checked": now, "success": success}, out)
                out.close()
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
    except OSError:
        pass
    return latest, error


def release_notice(latest):
    current, available = stable_version(__version__), stable_version(latest)
    if current and available and available > current:
        return f"slurm-wtf {latest} available (installed {__version__})"
    return ""


class UpdateCheck:
    def __init__(self):
        self.notice = ""
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        latest, _ = check_release()
        self.notice = release_notice(latest)
