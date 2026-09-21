import io
import json

import pytest

from slurm_wtf import cli, updates


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    monkeypatch.setattr(updates, "__version__", "0.2.0")
    monkeypatch.setattr(updates.time, "time", lambda: 1000000)


def response(*versions):
    return io.StringIO(json.dumps({"releases": {v: [{"yanked": False}] for v in versions}}))


def test_latest_installable_stable_release_and_cache(monkeypatch):
    calls = []

    def fetch(request, timeout):
        calls.append((request.full_url, timeout))
        return io.StringIO(
            json.dumps(
                {
                    "releases": {
                        "0.2.0": [{"yanked": False}],
                        "0.10.0": [{"yanked": False}],
                        "0.11.0rc1": [{"yanked": False}],
                        "1.0.0": [{"yanked": True}],
                        "2.0.0": [],
                    }
                }
            )
        )

    monkeypatch.setattr(updates, "urlopen", fetch)
    assert updates.check_release() == ("0.10.0", "")
    assert updates.check_release() == ("0.10.0", "")
    assert calls == [(updates.INDEX_URL, 2)]
    assert "uv tool upgrade slurm-wtf" in updates.release_notice("0.10.0")
    updates.check_release(force=True)
    assert len(calls) == 2


@pytest.mark.parametrize("version", ["0.2.0", "0.1.9", "0.3.0rc1", None, "garbage"])
def test_no_notice_for_current_older_or_invalid_versions(version):
    assert updates.release_notice(version) == ""


def test_offline_cached_and_retried_after_a_day(monkeypatch):
    calls = []

    def offline(*args, **kwargs):
        calls.append(1)
        raise OSError("offline")

    monkeypatch.setattr(updates, "urlopen", offline)
    assert "offline" in updates.check_release()[1]
    assert updates.check_release() == (None, "")
    assert len(calls) == 1
    monkeypatch.setattr(updates.time, "time", lambda: 1000000 + updates.CACHE_SECONDS)
    updates.check_release()
    assert len(calls) == 2


def test_stale_known_release_survives_network_failure(monkeypatch):
    monkeypatch.setattr(updates, "urlopen", lambda *a, **kw: response("0.3.0"))
    updates.check_release()
    monkeypatch.setattr(updates, "urlopen", lambda *a, **kw: io.StringIO("not JSON"))
    latest, error = updates.check_release(force=True)
    assert latest == "0.3.0"
    assert error


@pytest.mark.parametrize(
    "cache", ["not JSON", "[]", '{"checked": "invalid"}', '{"checked": 9999999}']
)
def test_bad_or_future_cache_is_refreshed(cache, monkeypatch):
    path = updates.cache_path()
    path.parent.mkdir(parents=True)
    path.write_text(cache)
    monkeypatch.setattr(updates, "urlopen", lambda *a, **kw: response("0.3.0"))
    assert updates.check_release() == ("0.3.0", "")


def test_unwritable_cache_does_not_hide_release(monkeypatch):
    path = updates.cache_path()
    path.parent.parent.mkdir(parents=True, exist_ok=True)
    path.parent.write_text("a file instead of a directory")
    monkeypatch.setattr(updates, "urlopen", lambda *a, **kw: response("0.3.0"))
    assert updates.check_release() == ("0.3.0", "")


def test_background_notice(monkeypatch):
    monkeypatch.setattr(updates, "urlopen", lambda *a, **kw: response("0.3.0"))
    check = updates.UpdateCheck()
    check.thread.join(timeout=2)
    assert "0.3.0 available" in check.notice


def test_explicit_check_needs_no_slurm(monkeypatch, capsys):
    monkeypatch.setattr(cli, "check_release", lambda force: ("0.3.0", ""))
    assert cli.main(["--check-updates"]) == 0
    assert "0.3.0 available" in capsys.readouterr().out
    monkeypatch.setattr(cli, "check_release", lambda force: (None, "offline"))
    assert cli.main(["--check-updates"]) == 1
    assert "offline" in capsys.readouterr().err


@pytest.mark.parametrize(
    "flags", [["--demo", "--plain"], ["--json"], ["--plain", "--no-update-check"]]
)
def test_disabled_checks_never_start_network(flags, monkeypatch, capsys):
    def unexpected():
        pytest.fail("update checker should not start")

    monkeypatch.setattr(cli, "UpdateCheck", unexpected)
    monkeypatch.setattr(cli, "load_model", lambda *a: {})
    monkeypatch.setattr(cli, "render", lambda *a: "snapshot")
    monkeypatch.setattr(cli, "to_json", lambda *a: "{}")
    assert cli.main(flags) == 0
    if "--json" in flags:
        assert json.loads(capsys.readouterr().out) == {}


def test_environment_opt_out(monkeypatch):
    monkeypatch.setenv("SLURM_WTF_NO_UPDATE_CHECK", "1")
    test_disabled_checks_never_start_network(["--plain"], monkeypatch, None)
