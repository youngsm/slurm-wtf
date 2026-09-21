from types import SimpleNamespace

import pytest

from slurm_wtf.cli import EVICT, GO, availability_detail, build, interactive_rows


@pytest.fixture
def capacity_snapshot():
    return {
        "cluster": "example",
        "mine": ["training|gpu|normal|"],
        "quota": ["root||||", "pool||node=7|normal|root", "training||gres/gpu=21|normal|pool"],
        "usage": [
            "ClusterName=example Account=root UserName= Partition= ID=1",
            "    GrpTRES=node=N(3),gres/gpu=N(7)",
            "ClusterName=example Account=pool UserName= Partition= ID=2",
            "    GrpTRES=node=7(3),gres/gpu=N(7)",
            "ClusterName=example Account=training UserName= Partition= ID=3",
            "    GrpTRES=node=N(3),gres/gpu=21(7)",
        ],
        "nodes": [f"n{i}|gpu|mixed|1/3/0/4|gpu:a100:4|gpu:a100:4|1000|1|" for i in range(8)],
        "jobs": [
            "1|filler|gpu|low|R|background|None|gres/gpu=12,node=3|1:00|filler|0:00|1:00|n[0-2]|",
            "2|training|gpu|normal|R|sam|None|gres/gpu=7,node=3|1:00|train|0:00|1:00|n[3-5]|",
        ],
    }


@pytest.mark.parametrize("idle", [0, 1])
@pytest.mark.parametrize("source", ["filler", "qos"])
def test_reclaimable_capacity_counts_with_or_without_idle_gpus(capacity_snapshot, idle, source):
    capacity_snapshot["nodes"][-1] = capacity_snapshot["nodes"][-1].replace(
        "|gpu:a100:4|1000", f"|gpu:a100:{4 - idle}|1000"
    )
    options = (
        {"ignore_users": ("background",)} if source == "filler" else {"preemptible_qos": ("low",)}
    )
    model = build(capacity_snapshot, "sam", **options)
    row = model["rows"][0]
    assert row["phys_free"] == idle
    assert row["reclaim"] == 12
    assert row["verdict"][0] == EVICT
    assert row["verdict"][4] == idle + 12
    assert "reclaimable" in row["verdict"][2]
    assert f"{idle} idle + 12 reclaimable" in availability_detail([row])
    args = SimpleNamespace(all_accounts=False, gpu_only=False, cpu_only=False, match=None)
    pool = interactive_rows(model, args, set())[0]
    assert pool["available"] == idle + 12
    assert pool["cells"][4] == f"{idle + 12} GPU"
    assert "reclaimable" in pool["detail"]


def test_filler_qos_overlap_not_double_counted_and_quota_still_limits(capacity_snapshot):
    capacity_snapshot["usage"][-1] = "    GrpTRES=node=N(3),gres/gpu=21(20)"
    row = build(capacity_snapshot, "sam", ignore_users=("background",), preemptible_qos=("low",))[
        "rows"
    ][0]
    assert row["reclaim"] == 12
    assert row["verdict"][4] == 1


def test_uncapped_normal_account_can_reclaim(capacity_snapshot):
    capacity_snapshot["usage"][3] = "    GrpTRES=node=N(3),gres/gpu=N(7)"
    capacity_snapshot["usage"][-1] = "    GrpTRES=node=N(3),gres/gpu=N(7)"
    row = build(capacity_snapshot, "sam", ignore_users=("background",))["rows"][0]
    assert row["verdict"][4] == 12


def test_do_not_request_preemption_when_idle_already_covers_quota(capacity_snapshot):
    capacity_snapshot["nodes"][-1] = capacity_snapshot["nodes"][-1].replace(
        "|gpu:a100:4|1000", "|gpu:a100:0|1000"
    )
    capacity_snapshot["usage"][-1] = "    GrpTRES=node=N(3),gres/gpu=21(20)"
    row = build(capacity_snapshot, "sam", ignore_users=("background",))["rows"][0]
    assert row["verdict"][0] == GO
    assert row["verdict"][4] == 1
