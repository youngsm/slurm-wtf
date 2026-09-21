import json
from types import SimpleNamespace

import pytest

from slurm_wtf.cli import (
    build,
    effective_caps,
    gpu_capacity,
    gpu_node_display,
    inline_rows,
    interactive_rows,
    node_equivalent,
    parse_tres,
    resource_caps,
    running_job_rows,
    to_json,
)


@pytest.fixture
def snapshot():
    # Parent usage includes a sibling account the inspected user cannot submit to.
    return {
        "mine": ["training|gpu|normal,preemptable|"],
        "quota": [
            "root||||",
            "research||node=7|normal|root",
            "training||gres/gpu=21|normal|research",
            "analysis||gres/gpu=21|normal|research",
        ],
        "usage": [
            "ClusterName=test Account=root UserName= Partition= ID=1",
            "    GrpTRES=node=N(7),gres/gpu=N(28)",
            "ClusterName=test Account=research UserName= Partition= ID=2",
            "    GrpTRES=node=7(7),gres/gpu=N(28)",
            "ClusterName=test Account=training UserName= Partition= ID=3",
            "    GrpTRES=node=N(0),gres/gpu=21(0)",
            "ClusterName=test Account=training UserName=sam(123) Partition=gpu ID=4",
            "    GrpTRES=node=N(0),gres/gpu=N(0)",
        ],
        "jobs": [],
        "nodes": [
            "a1|gpu|idle|0/64/0/64|gpu:a100:4|gpu:a100:0|1000|0|",
            "a2|gpu|idle|0/64/0/64|gpu:a100:4|gpu:a100:0|1000|0|",
            "a3|gpu|maint|0/0/64/64|(null)|(null)|1000|0|",
        ],
    }


@pytest.mark.parametrize("used,offer", [(7, 0), (6, 4), (5, 8)])
def test_parent_node_headroom_constrains_child(snapshot, used, offer):
    snapshot["usage"][3] = "    GrpTRES=node=7(%d),gres/gpu=N(23)" % used
    model = build(snapshot, "sam")
    row = model["rows"][0]
    assert row["own_caps"]["gpu"]["free"] == 21
    assert row["caps"]["node"]["used"] == used
    assert row["caps"]["node"]["source"] == "research"
    assert row["shared_limits"][0]["used"]["gpu"] == 23
    assert row["verdict"][4] == offer
    assert row["per_node"] == 4
    args = SimpleNamespace(all_accounts=False, gpu_only=False, cpu_only=False, match=None)
    collapsed = interactive_rows(model, args, set())
    assert len(collapsed) == 1
    assert collapsed[0]["expandable"]
    expanded = interactive_rows(model, args, {collapsed[0]["key"]})
    assert len(expanded) == 3
    assert expanded[2]["cells"][0].endswith("Other accounts")
    assert expanded[2]["cells"][2] == "23 GPU"
    assert expanded[1]["key"] == ("gpu", "training")
    assert expanded[1]["parent"] == collapsed[0]["key"]
    assert expanded[1]["available"] == offer
    assert expanded[0]["cells"][2] == "23 GPU"
    assert expanded[0]["cells"][3] == "5.75/7 node"
    assert expanded[0]["cells"][4] == ("0 GPU" if used == 7 else "4 GPU" if used == 6 else "5 GPU")
    assert expanded[1]["cells"][2] == "0 GPU"
    result = json.loads(to_json(model, args))
    assert len(result["shared_limits"]) == 1
    account = result["accounts"][0]
    assert account["offer_now"] == offer
    assert account["limit_sources"]["node"] == "research"


def test_missing_parent_counter_does_not_invent_free_capacity(snapshot):
    del snapshot["usage"][2:4]
    row = build(snapshot, "sam")["rows"][0]
    assert row["unknown_usage"] == ["research"]
    assert row["verdict"][4] == 0


def test_tightest_headroom_wins_and_zero_is_a_limit():
    scopes = [
        {"caps": resource_caps({"gpu": (21, 10), "cpu": (100, 10)}, True, "child")},
        {"caps": resource_caps({"gpu": (100, 95), "cpu": (0, 0)}, True, "parent")},
        {"caps": resource_caps({"gpu": (1000, 998)}, True, "grandparent")},
    ]
    caps = effective_caps(scopes)
    assert caps["gpu"]["free"] == 2
    assert caps["gpu"]["source"] == "grandparent"
    assert caps["cpu"]["cap"] == 0
    assert caps["cpu"]["free"] == 0


@pytest.mark.parametrize(
    "account,expected",
    [
        ("research", {"1", "2", "3"}),
        ("training", {"1"}),
        ("empty", set()),
        (("other", "gpu", "research", ("training",)), {"2", "3"}),
    ],
)
def test_running_jobs_match_account_subtree_and_partition(snapshot, account, expected):
    snapshot["jobs"] = [
        "1|training|gpu|normal|R|alice|None|gres/gpu=2,gres/gpu:a100=2|1:00:00|train|1-01:02:03|2-00:00:00|a1|",
        "2|analysis|gpu|normal|R|bob|None|gres/gpu=4|1:00:00|evaluate|02:03|UNLIMITED|a1|",
        "3|analysis|gpu|preemptable|R|background|None|gres/gpu=1|1:00:00|filler|01:03|2:00:00|a1|",
        "4|training|other-gpu|normal|R|alice|None|gres/gpu=1|1:00:00|other-partition|01:03|2:00:00|a1|",
        "5|training|gpu|normal|PD|alice|Priority|gres/gpu=1|1:00:00|pending|0:00|2:00:00|a1|",
        "6|unrelated|gpu|normal|R|alice|None|gres/gpu=1|1:00:00|other-account|01:03|2:00:00|a1|",
    ]
    model = build(snapshot, "sam")
    scope = account if isinstance(account, tuple) else ("gpu", account)
    jobs = running_job_rows(model, scope)
    args = SimpleNamespace(all_accounts=False, gpu_only=False, cpu_only=False, match=None)
    expanded = {("gpu", "research")}
    inline = inline_rows(model, args, expanded, {scope})
    assert {r["key"][-1] for r in inline if r["kind"] == "job"} == expected
    assert any(r["kind"] == "account" and r["key"] == ("gpu", "training") for r in inline)
    assert all(r["parent"] == scope for r in inline if r["kind"] == "job")
    assert not any(r["kind"] == "job" for r in inline_rows(model, args, expanded, set()))
    assert {job["key"][1] for job in jobs} == expected
    if "1" in expected:
        job = next(j for j in jobs if j["key"][1] == "1")
        assert job["cells"][:6] == ["1", "train", "alice", "1-01:02:03", "2-00:00:00", "2"]
    if "2" in expected:
        job = next(j for j in jobs if j["key"][1] == "2")
        assert job["cells"][4] == "UNLIMITED"
    assert sum(j["gpus"] for j in jobs) == sum({"1": 2, "2": 4, "3": 1}[jid] for jid in expected)


@pytest.mark.parametrize(
    "nodes,tres,value,bound",
    [
        ("a1", "gres/gpu=1,cpu=1,mem=100M", 0.25, "GPU"),
        ("a1", "gres/gpu=1,cpu=48,mem=100M", 0.75, "CPU"),
        ("a1", "gres/gpu=1,cpu=1,mem=900M", 0.9, "RAM"),
        ("a[1-2]", "gres/gpu=2,cpu=96,mem=800M", 1.5, "CPU"),
        ("missing", "gres/gpu=1", None, None),
    ],
)
def test_node_equivalent_uses_largest_resource_share(snapshot, nodes, tres, value, bound):
    resources = build(snapshot, "sam")["node_resources"]
    estimate = node_equivalent({"nodes": nodes, "tres": parse_tres(tres)}, resources)
    if value is None:
        assert estimate is None
    else:
        assert estimate["value"] == pytest.approx(value)
        assert estimate["bound"] == bound


@pytest.mark.parametrize("child_gpus,expected", [((4,), 4), ((4, 4), 8), ((4, 8), 8), ((0,), 0)])
def test_gpu_capacity_respects_child_limits_below_parent_node_limit(child_gpus, expected):
    model = {
        "parts": {"accelerator": {"density": 4}},
        "parents": {"root": "", "pool@accelerator": "root"},
        "account_limits": {("root", ""): {}, ("pool@accelerator", ""): {"node": 2}},
        "account_partitions": {},
    }
    for i, gpus in enumerate(child_gpus):
        account = "child%d@accelerator" % i
        model["parents"][account] = "pool@accelerator"
        model["account_limits"][(account, "")] = {"gpu": gpus}
    assert gpu_capacity(model, "pool@accelerator", "accelerator") == expected
    limit, remaining, free = gpu_node_display(model, "pool@accelerator", "accelerator", expected)
    assert limit == "%s/%s node" % (
        expected / 4 if expected % 4 else expected // 4,
        expected / 4 if expected % 4 else expected // 4,
    )
    assert remaining == "0 node"
    assert free == 0


@pytest.fixture
def generic_snapshot():
    from importlib.resources import files

    return json.loads(files("slurm_wtf").joinpath("demo.json").read_text())


def test_unrestricted_membership_keeps_partition_usage_and_global_limits(generic_snapshot):
    generic_snapshot["mine"] = ["training||batch|"]
    generic_snapshot["partitions"] = [
        "PartitionName=gpu AllowAccounts=research State=UP",
        "PartitionName=cpu AllowAccounts=research State=UP",
    ]
    model = build(generic_snapshot, "demo")
    rows = {r["partition"]: r for r in model["rows"]}
    assert set(rows) == {"gpu", "cpu"}
    assert rows["gpu"]["usage"]["gpu"] == 3
    assert rows["cpu"]["usage"]["gpu"] == 0
    assert rows["gpu"]["own_caps"]["gpu"]["used"] == 3
    assert rows["gpu"]["has_normal"]
    assert rows["cpu"]["has_normal"]
    assert rows["gpu"]["mine_run"]["jobs"] == 1
    assert rows["cpu"]["mine_run"]["jobs"] == 0
    args = SimpleNamespace(all_accounts=False, gpu_only=False, cpu_only=False, match=None)
    result = json.loads(to_json(model, args))
    assert {s["partition"] for s in result["shared_limits"]} == {"gpu", "cpu"}
    table_rows = interactive_rows(model, args, set())
    assert [r["cells"][1] for r in table_rows] == ["cpu", "gpu"]
    gpu_key = table_rows[1]["key"]
    model["favorites"] = {(model["cluster"],) + gpu_key}
    favorite_rows = interactive_rows(model, args, set())
    assert [r["cells"][1] for r in favorite_rows] == ["gpu", "cpu"]
    assert favorite_rows[0]["key"] == gpu_key
    assert "★" in favorite_rows[0]["cells"][0]
    model["favorites"] = {("another-cluster",) + gpu_key}
    assert [r["cells"][1] for r in interactive_rows(model, args, set())] == ["cpu", "gpu"]
    from slurm_wtf.cli import render

    args.no_jobs = True
    assert "SHARED POOLS" in render(model, args, 110)


def test_partition_access_uses_ancestors_and_custom_qos(generic_snapshot):
    generic_snapshot["mine"] = ["training||batch|"]
    generic_snapshot["partitions"] = [
        "PartitionName=gpu AllowAccounts=research AllowQos=batch AllowGroups=researchers State=UP",
        "PartitionName=cpu AllowAccounts=compute State=UP",
    ]
    model = build(generic_snapshot, "demo")
    assert [(r["account"], r["partition"]) for r in model["rows"]] == [("training", "gpu")]
    generic_snapshot["partitions"][0] = (
        "PartitionName=gpu AllowAccounts=research AllowQos=other State=UP"
    )
    assert build(generic_snapshot, "demo")["rows"] == []


def test_qos_names_and_ignored_users_are_opt_in(generic_snapshot):
    model = build(generic_snapshot, "demo")
    assert model["part_preempt"]["gpu"]["gpu"] == 0
    assert model["parts"]["gpu"]["gpu_used"] == 5
    configured = build(
        generic_snapshot, "demo", ignore_users=("alice",), preemptible_qos=("batch",)
    )
    assert configured["parts"]["gpu"]["gpu_used"] == 5
    assert configured["part_filler"]["gpu"]["gpu"] == 1
    assert configured["part_preempt"]["gpu"]["gpu"] == 4
    assert not any(r["has_normal"] for r in configured["rows"])


def test_cpu_only_and_unlimited_account(generic_snapshot):
    generic_snapshot["mine"] = ["compute|cpu|batch|"]
    model = build(generic_snapshot, "demo")
    assert model["rows"][0]["verdict"][4] == 112
    generic_snapshot["usage"][-1] = "    GrpTRES=cpu=N(16),node=N(1),gres/gpu=N(0)"
    row = build(generic_snapshot, "demo")["rows"][0]
    assert row["own_caps"] == {}
    assert row["verdict"][4] == 112
    assert row["verdict"][0] == 0


def test_controller_partition_records_do_not_overwrite_each_other():
    from slurm_wtf.cli import parse_assoc_mgr

    usage = parse_assoc_mgr(
        [
            "ClusterName=example Account=team UserName= Partition=gpu ID=1",
            "    GrpTRES=gres/gpu=8(3)",
            "ClusterName=example Account=team UserName= Partition=cpu ID=2",
            "    GrpTRES=cpu=64(16)",
            "ClusterName=example Account=team UserName=demo(1) Partition=gpu ID=3",
            "    GrpTRES=gres/gpu=1(1)",
        ]
    )
    assert usage == {("team", "gpu"): {"gpu": (8, 3)}, ("team", "cpu"): {"cpu": (64, 16)}}


@pytest.mark.parametrize(
    "gres,expected",
    [
        ("gpu:4", 4),
        ("gpu:a100:4", 4),
        ("gpu:2(IDX:0-1)", 2),
        ("gpu:a100:2(IDX:0-1),gpu:h100:4(IDX:2-5)", 6),
        ("(null)", 0),
    ],
)
def test_gpu_resources_accept_typed_and_untyped_forms(gres, expected):
    from slurm_wtf.cli import gres_count

    assert gres_count(gres) == expected


def test_access_lookup_is_scoped_to_related_pools(generic_snapshot):
    from slurm_wtf.cli import relevant_access_accounts

    generic_snapshot["quota"] += [
        "unrelated||node=100|batch|root",
        "outsider||gres/gpu=400|batch|unrelated",
    ]
    related = relevant_access_accounts(["training|gpu|batch|"], generic_snapshot["quota"])
    assert related == ["analysis", "research", "training"]


@pytest.mark.parametrize(
    "name,partition,pool,parent,expected",
    [
        ("team:_regular_@gpu", "gpu", True, None, "team"),
        ("team:_preemptable_@gpu", "gpu", True, None, "team (preempt)"),
        ("team:training@gpu", "gpu", False, "team:_regular_@gpu", "training"),
        ("research", "gpu", True, None, "research"),
        ("other:training@gpu", "gpu", False, "team:_regular_@gpu", "other:training"),
    ],
)
def test_compact_labels_preserve_exact_identifiers(name, partition, pool, parent, expected):
    from slurm_wtf.cli import account_label

    assert account_label(name, partition, pool=pool, parent=parent) == expected
    assert account_label(name, partition, pool=pool, parent=parent, full_names=True) == name
