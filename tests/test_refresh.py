import argparse

import pytest

from slurm_wtf import cli


@pytest.mark.parametrize(
    "text, seconds",
    [("150", 150), ("90s", 90), ("2.5m", 150), ("2.5M", 150), (".5m", 30), ("1h", 3600), ("1", 2)],
)
def test_parse_interval(text, seconds):
    assert cli.parse_interval(text) == seconds


@pytest.mark.parametrize("text", ["", "abc", "-5", "2.5x", "5 min"])
def test_parse_interval_rejects_garbage(text):
    with pytest.raises(argparse.ArgumentTypeError):
        cli.parse_interval(text)


def test_format_interval():
    assert cli.format_interval(150) == "2.5m"
    assert cli.format_interval(60) == "1m"
    assert cli.format_interval(45) == "45s"
    assert cli.format_interval(100) == "100s"


def _watch(monkeypatch, argv, env=None):
    seen = {}
    if env is not None:
        monkeypatch.setenv("SLURM_WTF_REFRESH", env)
    else:
        monkeypatch.delenv("SLURM_WTF_REFRESH", raising=False)

    def fake_interactive(args, update_check):
        seen["watch"], seen["default"] = args.watch, args.default_refresh
        return 0

    monkeypatch.setattr(cli, "interactive", fake_interactive)
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(cli.sys.stdout, "isatty", lambda: True)
    monkeypatch.setenv("TERM", "xterm")
    assert cli.main(["--demo", *argv]) == 0
    return seen


def test_default_refresh_is_two_and_a_half_minutes(monkeypatch):
    assert _watch(monkeypatch, []) == {"watch": None, "default": 150}
    assert _watch(monkeypatch, ["-w"]) == {"watch": 150, "default": 150}


def test_env_sets_default_and_flag_overrides(monkeypatch):
    assert _watch(monkeypatch, [], env="5m") == {"watch": None, "default": 300}
    assert _watch(monkeypatch, ["-w"], env="5m") == {"watch": 300, "default": 300}
    assert _watch(monkeypatch, ["-w", "30"], env="5m") == {"watch": 30, "default": 300}


def test_bad_env_is_reported(monkeypatch):
    with pytest.raises(SystemExit):
        _watch(monkeypatch, [], env="soon")
