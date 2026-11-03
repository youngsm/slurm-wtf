import json
from importlib.resources import files
from types import SimpleNamespace

import pytest

from slurm_wtf.cli import EVICT, build, inline_rows, preempt_mode, render, to_json

ARGS = dict(all_accounts=False, gpu_only=False, cpu_only=False, match=None)


@pytest.fixture
def demo():
    return json.loads(files("slurm_wtf").joinpath("demo.json").read_text())


def jobs_by_id(model):
    return {j["id"]: j for j in model["running_jobs"]}


@pytest.mark.parametrize(
    "layers,expected",
    [
        (("REQUEUE",), "requeue"),
        (("SUSPEND,GANG",), "suspend"),
        (("GANG",), None),
        (("OFF",), None),
        (("", None, "CANCEL"), "cancel"),
        (("", "OFF", "REQUEUE"), None),
        ((), None),
    ],
)
def test_preempt_mode_layers(layers, expected):
    assert preempt_mode(*layers) == expected


def test_qos_preemption_is_read_from_slurm(demo):
    model = build(demo, "demo")
    jobs = jobs_by_id(model)
    assert jobs["103"]["preempt"]["preemptible"] is True
    assert jobs["103"]["preempt"]["by_qos"] == ["batch"]
    assert jobs["103"]["preempt"]["mode"] == "requeue"
    assert jobs["101"]["preempt"]["preemptible"] is False
    assert model["part_preempt"]["gpu"]["gpu"] == 2
    rows = {r["account"]: r for r in model["rows"]}
    # batch preempts scavenger, so the scavenger job's GPUs are reclaimable.
    assert rows["training"]["reclaim"] == 2
    assert rows["training"]["reclaim_qos"] == ["batch"]
    assert rows["training"]["preemptible_qos"] == []


def test_preempt_column_and_detail(demo):
    model = build(demo, "demo")
    args = SimpleNamespace(**ARGS)
    entries = inline_rows(model, args, set(), {("gpu", "research")})
    header = next(r for r in entries if r["kind"] == "job_header")
    assert header["cells"][-1] == "PREEMPT?"
    cells = {r["cells"][0]: r for r in entries if r["kind"] == "job"}
    assert cells["103"]["cells"][-1] == "yes"
    assert cells["103"]["preemptible"]
    assert "preemptible by QoS batch (requeue)" in cells["103"]["detail"]
    assert "you can preempt via QoS batch" in cells["103"]["detail"]
    assert cells["101"]["cells"][-1] == "no"
    assert "not preemptible" in cells["101"]["detail"]


def test_own_preemptible_job_is_flagged(demo):
    demo["mine"][0] = "training|gpu|batch,scavenger|"
    demo["jobs"][0] = demo["jobs"][0].replace("|gpu|batch|R|", "|gpu|scavenger|R|")
    model = build(demo, "demo")
    row = next(r for r in model["rows"] if r["account"] == "training")
    assert row["preemptible_qos"] == ["scavenger"]
    args = SimpleNamespace(no_jobs=False, max_jobs=15, **ARGS)
    lines = [line for line in render(model, args, 110).splitlines() if "train-transformer" in line]
    assert "preemptible" in lines[0]
    mine = {j["id"]: j for j in json.loads(to_json(model, args))["my_jobs"]}
    assert mine["101"]["preemptible"] is True
    assert mine["101"]["preemptible_by_qos"] == ["batch"]
    assert mine["104"]["preemptible"] is False


def test_qos_preempt_mode_off_falls_back_to_cluster(demo):
    demo["qos"][1] = "scavenger||OFF"
    assert jobs_by_id(build(demo, "demo"))["103"]["preempt"]["preemptible"]
    demo["preempt"]["mode"] = "OFF"
    model = build(demo, "demo")
    assert not jobs_by_id(model)["103"]["preempt"]["preemptible"]
    assert all(r["reclaim"] == 0 for r in model["rows"])
    demo["qos"][1] = "scavenger||SUSPEND"
    assert jobs_by_id(build(demo, "demo"))["103"]["preempt"]["mode"] == "suspend"


def test_partition_preempt_mode_off_exempts_jobs(demo):
    demo["partitions"][0] += " PreemptMode=OFF"
    assert not jobs_by_id(build(demo, "demo"))["103"]["preempt"]["preemptible"]


def test_unreadable_qos_table_is_unknown_not_reclaimable(demo):
    demo["qos"] = None
    model = build(demo, "demo")
    assert jobs_by_id(model)["103"]["preempt"]["preemptible"] is None
    assert all(r["reclaim"] == 0 for r in model["rows"])
    entries = inline_rows(model, SimpleNamespace(**ARGS), set(), {("gpu", "research")})
    job = next(r for r in entries if r["kind"] == "job" and r["cells"][0] == "103")
    assert job["cells"][-1] == "?"


def test_reclaim_requires_one_of_the_rows_qos(demo):
    demo["qos"][0] = "batch||cluster"
    demo["qos"].append("urgent|scavenger|cluster")
    model = build(demo, "demo")
    assert jobs_by_id(model)["103"]["preempt"]["by_qos"] == ["urgent"]
    assert all(r["reclaim"] == 0 for r in model["rows"])


def partition_prio_snapshot(low_mode="REQUEUE", cluster_mode="OFF"):
    nodes = []
    for partition in ("high", "low"):
        nodes += [
            f"n1|{partition}|mixed|1/3/0/4|gpu:a100:4|gpu:a100:4|1000|1|",
            f"n2|{partition}|idle|0/4/0/4|gpu:a100:4|gpu:a100:0|1000|0|",
        ]
    return {
        "cluster": "x",
        "mine": ["team|high|normal|"],
        "quota": ["root||||", "team|||normal|root"],
        "usage": [
            "ClusterName=x Account=root UserName= Partition= ID=1",
            "    GrpTRES=node=N(1),gres/gpu=N(4)",
            "ClusterName=x Account=team UserName= Partition= ID=2",
            "    GrpTRES=node=N(0),gres/gpu=N(0)",
        ],
        "partitions": [
            "PartitionName=high PriorityTier=10 State=UP",
            "PartitionName=low PriorityTier=1 PreemptMode=%s State=UP" % low_mode,
        ],
        "preempt": {"type": "preempt/partition_prio", "mode": cluster_mode},
        "nodes": nodes,
        "jobs": ["1|other|low|normal|R|bob|None|gres/gpu=4,node=1|1:00|scav|0:00|1:00|n1|"],
    }


def test_partition_priority_preemption():
    model = build(partition_prio_snapshot(), "sam")
    job = model["running_jobs"][0]["preempt"]
    assert job["preemptible"] and job["by_partition"] == ["high"]
    row = model["rows"][0]
    assert row["partition"] == "high"
    assert row["reclaim"] == 4
    assert row["verdict"][0] == EVICT
    assert row["verdict"][4] == 8


def test_partition_priority_respects_partition_mode():
    model = build(partition_prio_snapshot(low_mode="OFF", cluster_mode="REQUEUE"), "sam")
    assert not model["running_jobs"][0]["preempt"]["preemptible"]
    assert model["rows"][0]["reclaim"] == 0


@pytest.mark.parametrize("qos_fails", [False, True])
def test_collect_reads_preemption_config(monkeypatch, demo, qos_fails):
    from slurm_wtf import cli

    calls = []

    def fake_sh(cmd, timeout=60):
        calls.append(cmd)
        if cmd[:3] == ["scontrol", "show", "config"]:
            return [
                "ClusterName             = example",
                "PreemptMode             = REQUEUE",
                "PreemptType             = preempt/qos",
            ]
        if cmd[:4] == ["sacctmgr", "-nP", "show", "qos"]:
            if qos_fails:
                raise SystemExit("sacctmgr: permission denied")
            return demo["qos"]
        if cmd[0] == "id":
            return ["researchers"]
        key = {"squeue": "jobs", "sinfo": "nodes"}.get(cmd[0])
        if key:
            return demo[key]
        if cmd[:3] == ["scontrol", "show", "partition"]:
            return demo["partitions"]
        if cmd[:3] == ["scontrol", "show", "assoc_mgr"]:
            return demo["usage"]
        if "user=demo" in cmd:
            return demo["mine"]
        if "user=" in cmd:
            return demo["quota"]
        return demo["access"]

    monkeypatch.setattr(cli, "sh", fake_sh)
    raw = cli.collect("demo")
    assert raw["preempt"] == {"type": "preempt/qos", "mode": "REQUEUE"}
    assert raw["qos"] == (None if qos_fails else demo["qos"])
    job = jobs_by_id(build(raw, "demo"))["103"]["preempt"]
    assert job["preemptible"] is (None if qos_fails else True)
