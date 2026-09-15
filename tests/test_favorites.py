import json

import pytest

from slurm_wtf.favorites import read_favorites, set_favorite


def test_favorites_persist_and_preserve_other_clusters(tmp_path):
    path = tmp_path / "settings" / "favorites.json"
    gpu = ("cluster-a", "gpu", "research")
    cpu = ("cluster-a", "cpu", "compute")
    other = ("cluster-b", "gpu", "research")
    assert read_favorites(path) == set()
    assert set_favorite(path, gpu, True) == {gpu}
    set_favorite(path, other, True)
    assert set_favorite(path, cpu, True) == {gpu, cpu, other}
    assert set_favorite(path, gpu, False) == {cpu, other}
    assert read_favorites(path) == {cpu, other}
    assert not list(path.parent.glob(".favorites-*"))


def test_invalid_favorites_are_not_overwritten(tmp_path):
    path = tmp_path / "favorites.json"
    path.write_text(json.dumps(["invalid"]))
    with pytest.raises(ValueError, match="Invalid favorites"):
        set_favorite(path, ("cluster", "gpu", "research"), True)
    assert path.read_text() == '["invalid"]'
