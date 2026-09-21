import json
from types import SimpleNamespace

import pytest

from slurm_wtf import cli, settings


def test_settings_are_cluster_scoped_and_clearable(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    assert settings.read_filler_users("alpha") == ()
    settings.save_filler_users("alpha", ["background", "background"])
    settings.save_filler_users("beta", ["other"])
    assert settings.read_filler_users("alpha") == ("background",)
    assert settings.read_filler_users("beta") == ("other",)
    assert settings.read_filler_users("gamma") == ()
    settings.save_filler_users("alpha", [])
    assert settings.read_filler_users("alpha") == ()
    assert settings.read_filler_users("beta") == ("other",)


def test_saved_setting_is_default_but_explicit_empty_overrides(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    settings.save_filler_users("alpha", ["background"])
    monkeypatch.setattr(cli, "collect", lambda user: {"cluster": "alpha"})
    monkeypatch.setattr(cli, "build", lambda raw, user, ignore, qos: ignore)
    args = SimpleNamespace(demo=False, user="sam", preemptible_qos="")
    assert cli.load_model(args, None) == ("background",)
    assert cli.load_model(args, ()) == ()
    assert cli.load_model(args, ("another",)) == ("another",)


def test_invalid_settings_are_not_overwritten(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    path = settings.save_filler_users("alpha", [])
    path.write_text(json.dumps({"alpha": "not a list"}))
    with pytest.raises(ValueError):
        settings.save_filler_users("beta", ["background"])
    assert json.loads(path.read_text()) == {"alpha": "not a list"}


def test_save_command(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setattr(cli, "current_cluster", lambda: "alpha")
    assert cli.main(["--save-filler-users", " background, other "]) == 0
    assert settings.read_filler_users("alpha") == ("background", "other")
    assert "alpha" in capsys.readouterr().out
