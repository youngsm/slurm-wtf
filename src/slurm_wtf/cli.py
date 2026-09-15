#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Interactive account capacity and running jobs for Slurm clusters."""

import argparse
import getpass
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files

from . import __version__

# ---------------------------------------------------------------------------
# style
# ---------------------------------------------------------------------------


# Status colours are reserved: they mean state, never identity, and each is
# always paired with a glyph and a word, so colour is never the only channel.
# Mid-lightness 256-colour tones stay legible on light *and* dark terminals.
class C(object):
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[38;5;242m"
    RULE = "\033[38;5;238m"
    INK = "\033[38;5;252m"
    MUTED = "\033[38;5;245m"
    OK = "\033[38;5;78m"  # go
    WARN = "\033[38;5;179m"  # degraded / evictable
    BAD = "\033[38;5;167m"  # blocked
    ACCENT = "\033[38;5;146m"

    @classmethod
    def strip(cls):
        for k in list(vars(cls)):
            if k.isupper():
                setattr(cls, k, "")


BLOCKS = " ▏▎▍▌▋▊▉█"
ANSI_RE = re.compile(r"\033\[[0-9;]*m")


def bar(frac, width, color):
    """A baseline-anchored meter with 1/8-cell resolution."""
    frac = 0.0 if frac < 0 else (1.0 if frac > 1 else frac)
    cells = frac * width
    full = int(cells)
    rem = int(round((cells - full) * 8))
    if rem == 8:
        full, rem = full + 1, 0
    out = "█" * full
    if rem and full < width:
        out += BLOCKS[rem]
    # A nonzero-but-tiny value still deserves one visible pixel.
    if not out and frac > 0:
        out = BLOCKS[1]
    return color + out + C.RULE + "·" * max(0, width - len(out)) + C.RESET


def vis_len(s):
    return len(ANSI_RE.sub("", s))


def pad(s, width, align="<"):
    gap = width - vis_len(s)
    if gap <= 0:
        return s
    return (" " * gap + s) if align == ">" else (s + " " * gap)


def fmt_n(x):
    """Compact integer-ish formatting: 4, 21, 1.2k."""
    x = float(x)
    if abs(x) >= 10000:
        return "%.1fk" % (x / 1000.0)
    return "%d" % int(x) if x == int(x) else "%.1f" % x


def heat(frac):
    return C.OK if frac < 0.7 else (C.WARN if frac < 0.92 else C.BAD)


# ---------------------------------------------------------------------------
# slurm plumbing
# ---------------------------------------------------------------------------


# `-O "Field:|"` yields untruncated pipe-delimited output, unlike the fixed-width
# default -- the only reliable way to read whole TRES strings back out.
def sh(cmd, timeout=60):
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate(timeout=timeout)
    except OSError as exc:
        raise SystemExit("cannot run %s: %s" % (cmd[0], exc))
    except subprocess.TimeoutExpired:
        p.kill()
        raise SystemExit("%s timed out after %ss" % (cmd[0], timeout))
    if p.returncode != 0:
        raise SystemExit("%s failed: %s" % (" ".join(cmd), err.decode("utf8", "replace").strip()))
    return out.decode("utf8", "replace").splitlines()


def split_rows(lines, n):
    rows = []
    for line in lines:
        f = line.split("|")
        if len(f) >= n:
            rows.append([x.strip() for x in f[:n]])
    return rows


_MEM_UNIT = {"K": 1.0 / 1024, "M": 1.0, "G": 1024.0, "T": 1024.0**2, "P": 1024.0**3}
RESOURCES = ("gpu", "node", "cpu", "mem")


def zero():
    d = dict((k, 0.0) for k in RESOURCES)
    d["jobs"] = 0
    return d


def parse_tres(s):
    """'cpu=8,mem=80G,node=1,gres/gpu=4' -> {'cpu':8,'mem':81920,'node':1,'gpu':4}

    mem normalises to MB; typed `gres/gpu:a100` is a subset of the untyped
    `gres/gpu` total, so only the latter is summed.
    """
    out = zero()
    if not s or s in ("(null)", "None"):
        return out
    for item in s.split(","):
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        k, v = k.strip().lower(), v.strip()
        if k == "mem":
            m = re.match(r"^([0-9.]+)\s*([KMGTP])?", v, re.I)
            if m:
                out["mem"] += float(m.group(1)) * _MEM_UNIT.get((m.group(2) or "M").upper(), 1.0)
        elif k == "gres/gpu":
            out["gpu"] += float(re.sub(r"[^0-9.]", "", v) or 0)
        elif k in ("cpu", "node"):
            out[k] += float(re.sub(r"[^0-9.]", "", v) or 0)
    return out


def add_into(dst, src):
    for k in dst:
        dst[k] += src.get(k, 0)


def gres_count(s):
    """Sum GPUs out of 'gpu:a100:4' / 'gpu:l40s:9(IDX:0-8)'."""
    if not s or s.startswith("(null)"):
        return 0
    return sum(int(n) for n in re.findall(r"(?:^|,)gpu:(?:[^:,()]+:)?(\d+)", s))


# Pending for one of these is not contention: the job is not competing for
# resources, so counting it as queue pressure would be a lie.
NOT_WAITING_ON_RESOURCES = {
    "JobHeldUser",
    "JobHeldAdmin",
    "Dependency",
    "DependencyNeverSatisfied",
    "BeginTime",
    "JobArrayTaskLimit",
    "None",
    "",
}

# A node in one of these cannot take work, so it must not inflate capacity.
DOWN_STATES = (
    "down",
    "drain",
    "drng",
    "fail",
    "maint",
    "unk",
    "boot",
    "power",
    "resv",
    "invalid",
    "future",
)

FILLER_USERS = ()

JOB_FMT = (
    "JobID:|,Account:|,Partition:|,QOS:|,StateCompact:|,UserName:|,"
    "Reason:|,tres-alloc:|,TimeLeft:|,Name:|,TimeUsed:|,TimeLimit:|,NodeList:|"
)
NODE_FMT = (
    "NodeHost:|,Partition:|,StateLong:|,CPUsState:|,Gres:|,GresUsed:|,"
    "Memory:|,AllocMem:|,Features:|"
)


def account_chain(account, parents):
    """Follow Slurm's actual hierarchy, including ancestors outside user membership."""
    chain = []
    while account:
        if account in chain:
            raise ValueError("cycle in Slurm account hierarchy: " + account)
        chain.append(account)
        if account not in parents:
            raise ValueError("missing Slurm account association: " + account)
        account = parents[account]
    return chain


def collect(user):
    """Query the current cluster, including memberships without partition restrictions."""
    config = sh(["scontrol", "show", "config"])
    cluster = next(
        (
            line.split("=", 1)[1].strip()
            for line in config
            if line.strip().startswith("ClusterName") and "=" in line
        ),
        None,
    )
    if not cluster:
        raise SystemExit("scontrol did not report a ClusterName")
    cmds = {
        "mine": [
            "sacctmgr",
            "-nP",
            "show",
            "assoc",
            "where",
            "cluster=" + cluster,
            "user=" + user,
            "format=Account,Partition,QOS,GrpTRES",
        ],
        "quota": [
            "sacctmgr",
            "-nP",
            "show",
            "assoc",
            "where",
            "cluster=" + cluster,
            "user=",
            "format=Account,Partition,GrpTRES,QOS,ParentName",
        ],
        "partitions": ["scontrol", "show", "partition", "-o"],
        "jobs": ["squeue", "--local", "-h", "-a", "-t", "all", "-O", JOB_FMT],
        "nodes": ["sinfo", "--local", "-h", "-N", "-O", NODE_FMT],
    }
    with ThreadPoolExecutor(max_workers=len(cmds)) as pool:
        futs = {k: pool.submit(sh, v) for k, v in cmds.items()}
        raw = {k: f.result() for k, f in futs.items()}
    parents = {r[0]: r[4] for r in split_rows(raw["quota"], 5)}
    accounts = set()
    for row in split_rows(raw["mine"], 4):
        accounts.update(account_chain(row[0], parents))
    access_accounts = relevant_access_accounts(raw["mine"], raw["quota"])
    detail_queries = {}
    if accounts:
        detail_queries["usage"] = [
            "scontrol",
            "show",
            "assoc_mgr",
            "accounts=" + ",".join(sorted(accounts)),
            "flags=assoc",
        ]
    if access_accounts:
        detail_queries["access"] = [
            "sacctmgr",
            "-nP",
            "show",
            "assoc",
            "where",
            "cluster=" + cluster,
            "accounts=" + ",".join(access_accounts),
            "format=Account,User,Partition",
        ]
    raw.update(usage=[], access=[])
    with ThreadPoolExecutor(max_workers=max(1, len(detail_queries))) as pool:
        futures = {key: pool.submit(sh, command) for key, command in detail_queries.items()}
        raw.update({key: future.result() for key, future in futures.items()})
    raw["cluster"] = cluster
    restricted_groups = any(
        p.get("AllowGroups", "ALL") != "ALL" for p in parse_partitions(raw["partitions"]).values()
    )
    raw["groups"] = sh(["id", "-Gn", user])[0].split() if restricted_groups else []
    return raw


def relevant_access_accounts(memberships, quotas):
    """Request memberships only beneath the user's nearest quota-bearing pools."""
    records = split_rows(quotas, 5)
    parents = {row[0]: row[4] for row in records}
    limited = {row[0] for row in records if row[2]}
    children = defaultdict(list)
    for account, parent in parents.items():
        children[parent].append(account)
    roots = set()
    for account, _, _, _ in split_rows(memberships, 4):
        chain = account_chain(account, parents)
        roots.add(next((ancestor for ancestor in chain[1:] if ancestor in limited), account))
    pending, related = list(roots), set()
    while pending:
        account = pending.pop()
        if account in related:
            continue
        related.add(account)
        pending.extend(children[account])
    return sorted(related)


def partition_access(raw, parents, parts):
    """Derive account reachability from associations, never from account names."""
    access = defaultdict(set)
    for account, user, partition in split_rows(raw.get("access", []), 3):
        if user:
            access[account].update([partition] if partition else parts)
    for account, partition, _, _ in split_rows(raw["mine"], 4):
        access[account].update([partition] if partition else parts)
    for account, partition, _, _, _ in split_rows(raw["quota"], 5):
        if partition:
            access[account].add(partition)
    parent_accounts = set(parents.values())
    for account in parents:
        if account not in parent_accounts and not access[account]:
            access[account].update(parts)
    for account, partitions in list(access.items()):
        for ancestor in account_chain(account, parents)[1:]:
            access[ancestor].update(partitions)
    return access


def parse_partitions(lines):
    result = {}
    for line in lines:
        fields = dict(re.findall(r"(\w+)=([^ ]*)", line))
        if "PartitionName" in fields:
            result[fields["PartitionName"]] = fields
    return result


def allowed_partition(account, qos, partition, parents, groups):
    """Apply published partition account, group and QoS access lists."""
    ancestors = set(account_chain(account, parents))
    for dimension, values in (("Accounts", ancestors), ("Groups", set(groups))):
        allowed = partition.get("Allow" + dimension, "ALL")
        denied = set(partition.get("Deny" + dimension, "").split(","))
        if allowed != "ALL":
            if not values.intersection(allowed.split(",")):
                return False
        elif values & denied:
            return False
    allowed = partition.get("AllowQos", "ALL")
    if qos and allowed != "ALL" and not set(qos) & set(allowed.split(",")):
        return False
    if qos and set(qos) <= set(partition.get("DenyQos", "").split(",")):
        return False
    return partition.get("State", "UP") != "DOWN"


_LIMIT_RE = re.compile(r"(cpu|mem|node|gres/gpu)=([N0-9]+)\((\d+)\)")


def parse_assoc_mgr(lines):
    """account -> {res: (limit or None, used)} from the account-level rows.

    `scontrol show assoc_mgr` prints `GrpTRES=cpu=220(20),node=6(2),...` per
    association: limit outside the parens (N = none), live usage inside. Only the
    rows with an empty UserName carry the account-wide counter.
    """
    out, acct = {}, None
    for line in lines:
        if line.startswith("ClusterName="):
            m = re.search(r"\bAccount=(\S+) UserName=(\S*) Partition=(\S*)", line + " ")
            acct = (m[1], m[3]) if m and m[2] == "" else None
        elif acct and line.strip().startswith("GrpTRES="):
            d = {}
            for k, lim, used in _LIMIT_RE.findall(line):
                k = "gpu" if k == "gres/gpu" else k
                d[k] = (None if lim == "N" else float(lim), float(used))
            out[acct] = d
            acct = None
    return out


def resource_caps(live, is_gpu, source):
    return dict(
        (
            k,
            {
                "cap": lim,
                "used": used,
                "free": max(0.0, lim - used),
                "frac": min(1.0, used / lim) if lim else 1.0,
                "source": source,
            },
        )
        for k, (lim, used) in live.items()
        if lim is not None and (k != "gpu" or is_gpu)
    )


def effective_caps(scopes):
    """Each ancestor constrains headroom independently; never sum sibling quotas."""
    caps = {}
    for scope in scopes:
        for res, cap in scope["caps"].items():
            if res not in caps or cap["free"] < caps[res]["free"]:
                caps[res] = cap
    return caps


def short_gpu(model, features):
    """'geforce_rtx_2080_ti' + 'GPU_MEM:11GB' -> 'rtx2080ti 11GB'."""
    name = model.replace("geforce_", "").replace("_", "")
    mem = ""
    m = re.search(r"GPU_MEM:(\S+?)(?:,|$)", features or "")
    if m:
        mem = " " + m.group(1)
    return name + mem


# ---------------------------------------------------------------------------
# model
# ---------------------------------------------------------------------------


def build(raw, user, ignore_users=FILLER_USERS, preemptible_qos=()):
    # -- partitions: physical truth -----------------------------------------
    parts, node_resources = {}, {}
    for host, part, state, cpustate, gres, gresused, mem, allocmem, feats in split_rows(
        raw["nodes"], 9
    ):
        part = part.rstrip("*")
        p = parts.setdefault(
            part,
            {
                "name": part,
                "nodes": 0,
                "nodes_down": 0,
                "gpu": 0.0,
                "gpu_used": 0.0,
                "gpu_cfg": 0.0,
                "cpu": 0.0,
                "cpu_used": 0.0,
                "cpu_cfg": 0.0,
                "node_cfg": 0.0,
                "gpu_nodes": 0,
                "models": defaultdict(int),
                "gpu_desc": "",
            },
        )
        ngpu = gres_count(gres)
        try:
            alloc, idle, other, total = [int(x) for x in cpustate.split("/")]
        except ValueError:
            alloc = idle = total = 0
        node_resources[host] = {"gpu": ngpu, "cpu": total, "mem": float(mem)}
        p["nodes"] += 1
        # Configured totals include down nodes: they are the right denominator
        # for "how oversubscribed is this partition structurally".
        p["gpu_cfg"] += ngpu
        p["gpu_nodes"] += bool(ngpu)
        p["cpu_cfg"] += total
        for model, n in re.findall(r"gpu:([^:,]+):(\d+)", gres or ""):
            p["models"][model] += int(n)
            if not p["gpu_desc"]:
                p["gpu_desc"] = short_gpu(model, feats)
        low = state.lower()
        if any(d in low for d in DOWN_STATES):
            p["nodes_down"] += 1
            continue
        p["cpu"] += alloc + idle
        p["cpu_used"] += alloc
        p["gpu"] += ngpu
        p["gpu_used"] += gres_count(gresused)
    for p in parts.values():
        p["node_cfg"] = p["nodes"]
        p["node"] = p["nodes"] - p["nodes_down"]
        p["is_gpu"] = p["gpu_cfg"] > 0

    # -- jobs: who is using, who is waiting ---------------------------------
    pend = defaultdict(zero)  # account -> pending TRES (real contention)
    mine_run = defaultdict(zero)
    mine_pend = defaultdict(zero)
    part_preempt = defaultdict(zero)  # partition -> TRES a normal job can reclaim
    part_filler = defaultdict(zero)  # partition -> TRES held by filler jobs
    part_pend = defaultdict(zero)
    observed = defaultdict(set)
    my_jobs, running_jobs = [], []

    for (
        jid,
        acct,
        part,
        qos,
        st,
        who,
        reason,
        tres,
        left,
        name,
        elapsed,
        limit,
        nodes,
    ) in split_rows(raw["jobs"], 13):
        part = part.rstrip("*")
        observed[acct].add(part)
        t = parse_tres(tres)
        t["jobs"] = 1
        if st == "R":
            running_jobs.append(
                {
                    "id": jid,
                    "account": acct,
                    "partition": part,
                    "qos": qos,
                    "user": who,
                    "name": name,
                    "elapsed": elapsed,
                    "limit": limit,
                    "tres": t,
                    "nodes": nodes,
                }
            )
        if who in ignore_users:
            if st == "R":
                add_into(part_filler[part], t)
            continue
        if st == "R":
            if qos in preemptible_qos:
                add_into(part_preempt[part], t)
            if who == user:
                add_into(mine_run[(acct, part)], t)
        elif st == "PD":
            if reason not in NOT_WAITING_ON_RESOURCES:
                add_into(pend[(acct, part)], t)
                add_into(part_pend[part], t)
            if who == user:
                add_into(mine_pend[(acct, part)], t)
        if who == user and st in ("R", "PD"):
            my_jobs.append(
                {
                    "id": jid,
                    "account": acct,
                    "partition": part,
                    "qos": qos,
                    "state": st,
                    "reason": reason,
                    "tres": t,
                    "left": left,
                    "name": name,
                }
            )

    # sinfo counts filler allocations as busy; they are not.
    for pn, f in part_filler.items():
        if pn in parts:
            for res in ("gpu", "cpu"):
                parts[pn][res + "_used"] = max(0.0, parts[pn][res + "_used"] - f[res])
                parts[pn][res + "_filler"] = f[res]

    # Node-equivalent free capacity, partition-wide: however many whole nodes'
    # worth of the primary device this partition's free devices add up to.
    # "Open nodes" regardless of whose account could claim them.
    for p in parts.values():
        res = "gpu" if p["is_gpu"] else "cpu"
        device_nodes = p["gpu_nodes"] if p["is_gpu"] else p["node_cfg"]
        density = (p[res + "_cfg"] / device_nodes) if device_nodes else 0.0
        p["density"] = density
        p["free_nodes"] = ((p[res] - p[res + "_used"]) / density) if density else 0.0

    # -- quotas -------------------------------------------------------------
    quota, parents, account_limits = {}, {}, {}
    for acct, _part, grptres, _qos, parent in split_rows(raw["quota"], 5):
        parents[acct] = parent
        declared = {item.split("=", 1)[0] for item in grptres.split(",")}
        parsed = parse_tres(grptres)
        account_limits[(acct, _part)] = {
            k: parsed[k] for k in RESOURCES if ("gres/gpu" if k == "gpu" else k) in declared
        }
        if grptres:
            quota[(acct, _part)] = parse_tres(grptres)

    access = partition_access(raw, parents, parts)
    partition_rules = parse_partitions(raw.get("partitions", []))

    # Structural oversubscription: if every account cashed its quota at once,
    # how many times over would the partition be booked?
    for p in parts.values():
        p["booked_gpu"] = 0.0
        p["booked_cpu"] = 0.0
    parent_accounts = set(parents.values())
    for (acct, explicit_part), q in quota.items():
        if acct in parent_accounts:
            continue
        candidates = {explicit_part} if explicit_part else access[acct]
        pn = next(iter(candidates)) if len(candidates) == 1 else None
        if pn is None:
            obs = observed.get(acct) or set()
            pn = list(obs)[0] if len(obs) == 1 else None
        if pn:
            parts[pn]["booked_gpu"] += q["gpu"]
            parts[pn]["booked_cpu"] += q["cpu"]

    partition_usage = defaultdict(zero)
    occupied = defaultdict(set)
    for job in running_jobs:
        if job["account"] not in parents:
            continue
        for account in account_chain(job["account"], parents):
            key = (account, job["partition"])
            add_into(partition_usage[key], job["tres"])
            occupied[key].update(expand_hosts(job["nodes"]))
    for key, hosts in occupied.items():
        partition_usage[key]["node"] = len(hosts)

    # -- my associations ----------------------------------------------------
    usage = parse_assoc_mgr(raw["usage"])
    account_limits.update(
        {
            acct: {k: lim for k, (lim, _) in live.items() if lim is not None}
            for acct, live in usage.items()
        }
    )
    rows, seen = [], set()
    memberships = []
    for acct, partition, qos, grptres in split_rows(raw["mine"], 4):
        for part in [partition.rstrip("*")] if partition else sorted(parts):
            if allowed_partition(
                acct,
                qos.split(",") if qos else [],
                partition_rules.get(part, {}),
                parents,
                raw.get("groups", []),
            ):
                memberships.append((acct, part, qos, grptres))
    for acct, part, qos, grptres in memberships:
        if (acct, part) in seen:
            continue
        seen.add((acct, part))
        pinfo = parts.get(part)
        if pinfo is None:
            continue
        res = "gpu" if pinfo["is_gpu"] else "cpu"
        scopes, unknown = [], []
        for source in account_chain(acct, parents):
            live = usage.get((source, part), usage.get((source, "")))
            if live is None:
                unknown.append(source)
                continue
            quota_used = {k: v[1] for k, v in live.items()}
            global_usage = (source, part) not in usage and len(access[source]) > 1
            scopes.append(
                {
                    "account": source,
                    "partition": part,
                    "shared_partitions": sorted(access[source]),
                    "used": partition_usage[(source, part)] if global_usage else quota_used,
                    "quota_used": quota_used,
                    "usage_source": "jobs" if global_usage else "controller",
                    "caps": resource_caps(live, pinfo["is_gpu"], source),
                }
            )
        live = usage.get((acct, part), usage.get((acct, ""), {}))
        own_caps = resource_caps(live, pinfo["is_gpu"], acct)
        caps = effective_caps(scopes)
        preempt_used = sum(
            j["tres"][res]
            for j in running_jobs
            if j["account"] == acct and j["partition"] == part and j["qos"] in preemptible_qos
        )
        rows.append(
            {
                "account": acct,
                "partition": part,
                "qos": [x for x in qos.split(",") if x],
                "res": res,
                "caps": caps,
                "own_caps": own_caps,
                "shared_limits": [
                    scope for scope in scopes if scope["account"] != acct and scope["caps"]
                ],
                "unknown_usage": unknown,
                "usage": scopes[0]["used"] if scopes and scopes[0]["account"] == acct else {},
                "quota_usage": dict((k, value[1]) for k, value in live.items()),
                "primary": res if res in own_caps else None,
                "preempt_used": preempt_used,
                "pending": pend.get((acct, part), zero()),
                "mine_run": mine_run.get((acct, part), zero()),
                "mine_pend": mine_pend.get((acct, part), zero()),
                "phys_free": pinfo[res] - pinfo[res + "_used"],
                "reclaim": part_preempt[part][res],
                "per_node": pinfo["density"],
                "part": pinfo,
                "has_normal": not qos or any(q not in preemptible_qos for q in qos.split(",")),
            }
        )
    for r in rows:
        r["verdict"] = verdict(r)
        # How many whole nodes' worth of the account's offer this is, so GPU
        # and CPU accounts (and different GPU generations) can be compared
        # on one scale: "how many nodes could I actually get right now".
        r["offer_nodes"] = (r["verdict"][4] / r["per_node"]) if r["per_node"] else 0.0
    return {
        "parts": parts,
        "rows": rows,
        "my_jobs": my_jobs,
        "part_pend": part_pend,
        "running_jobs": running_jobs,
        "parents": parents,
        "node_resources": node_resources,
        "account_limits": account_limits,
        "account_partitions": access,
        "cluster": raw.get("cluster", ""),
        "part_preempt": part_preempt,
        "part_filler": part_filler,
        "user": user,
    }


# Verdict ranks, best first. Also the sort order and the headline pick.
GO, EVICT, TIGHT, PREEMPTONLY, FULL, NONE = range(6)
UNIT = {"gpu": "GPU", "cpu": "CPU", "node": "node", "mem": "MB memory"}


def verdict(r):
    """(rank, glyph, label, colour, offer) -- colour is never the only signal.

    `offer` is how much of the partition's primary resource (GPUs, or CPUs on a
    CPU partition) you could realistically get from this account right now.
    """
    caps, res, unit = r["caps"], r["res"], UNIT[r["res"]]
    if r["unknown_usage"]:
        return (NONE, "?", "usage unavailable: " + ", ".join(r["unknown_usage"]), C.WARN, 0)
    if not caps:
        available = max(0.0, r["phys_free"])
        rank = GO if available and r["has_normal"] else PREEMPTONLY if available else TIGHT
        return (
            rank,
            "○",
            "no account cap · %s %s idle" % (fmt_n(available), unit),
            C.OK if available else C.DIM,
            available,
        )

    # Every limited dimension can stop you; report the first one that has.
    exhausted = [k for k in ("gpu", "node", "cpu", "mem") if k in caps and caps[k]["free"] < 1]
    if exhausted:
        tail = " · %d queued" % r["pending"]["jobs"] if r["pending"]["jobs"] else ""
        key = exhausted[0]
        source = caps[key]["source"]
        scope = "shared %s" % source if source != r["account"] else "account"
        if key == "node":
            return (TIGHT, "◑", "%s node limit reached%s" % (scope, tail), C.WARN, 0)
        return (FULL, "×", "%s %s limit full%s" % (scope, UNIT[key], tail), C.BAD, 0)

    # Translate the free quota in each dimension into primary-resource units.
    free = caps[res]["free"] if res in caps else float("inf")
    if "node" in caps and r["per_node"]:
        free = min(free, caps["node"]["free"] * r["per_node"])
    if free == float("inf"):
        free = r["phys_free"]
    if not r["has_normal"]:
        # Quota exists but the association only carries the preemptable QOS.
        return (
            PREEMPTONLY,
            "◐",
            "%s %s free · preemptable qos only"
            % (fmt_n(min(free, r["phys_free"])) if r["phys_free"] >= 1 else fmt_n(free), unit),
            C.WARN,
            min(free, r["phys_free"]),
        )
    available = min(free, max(0.0, r["phys_free"]) + r["reclaim"])
    if r["pending"]["jobs"] and r["pending"][res] >= free:
        return (
            TIGHT,
            "◑",
            "%s %s free · %d job%s queued ahead"
            % (fmt_n(free), unit, r["pending"]["jobs"], "s" if r["pending"]["jobs"] > 1 else ""),
            C.WARN,
            available,
        )
    if r["phys_free"] >= 1:
        return (
            GO,
            "●",
            "%s %s free now" % (fmt_n(min(free, r["phys_free"])), unit),
            C.OK,
            min(free, r["phys_free"]),
        )
    if r["reclaim"] >= 1:
        return (
            EVICT,
            "◕",
            "%s %s free · evicts preemptable" % (fmt_n(available), unit),
            C.OK,
            available,
        )
    return (TIGHT, "◑", "%s %s in quota · partition full" % (fmt_n(free), unit), C.WARN, 0)


# ---------------------------------------------------------------------------
# render
# ---------------------------------------------------------------------------


def select(model, args):
    rows = [
        r
        for r in model["rows"]
        if r["own_caps"] or r["has_normal"] or r["unknown_usage"] or args.all_accounts
    ]
    if args.gpu_only:
        rows = [r for r in rows if r["res"] == "gpu"]
    if args.cpu_only:
        rows = [r for r in rows if r["res"] == "cpu"]
    if args.match:
        pat = re.compile(args.match, re.I)
        rows = [r for r in rows if pat.search(r["account"]) or pat.search(r["partition"])]
    # GPU accounts first (still a different pool of hardware than CPU), then
    # by node-equivalent offer: that scale is comparable across resource
    # types and GPU generations, unlike a raw GPU/CPU count.
    rows.sort(key=lambda r: (r["res"] != "gpu", r["verdict"][0], -r["offer_nodes"], r["account"]))
    return rows


def account_label(account, partition, *, pool=False, parent=None, full_names=False):
    """Shorten display labels only; Slurm identifiers remain untouched."""
    if full_names:
        return account
    suffix = "@" + partition
    name = account[: -len(suffix)] if account.endswith(suffix) else account
    if pool:
        match = re.fullmatch(r"([^:]+):_([^:]+)_", name)
        if match:
            namespace, qualifier = match.groups()
            if qualifier == "regular":
                return namespace
            qualifier = "preempt" if qualifier == "preemptable" else qualifier
            return namespace + " (" + qualifier + ")"
    if parent and ":" in parent:
        prefix = parent.split(":", 1)[0] + ":"
        if name.startswith(prefix):
            return name[len(prefix) :]
    return name


def pool_groups(rows):
    groups = {}
    for row in rows:
        scopes = row["shared_limits"]
        scope = next(
            (s for s in scopes if s["partition"] == row["partition"]), scopes[0] if scopes else None
        )
        key = (row["partition"], scope["account"] if scope else row["account"])
        groups.setdefault(key, {"scope": scope, "rows": []})["rows"].append(row)
    return sorted(
        groups.values(),
        key=lambda g: (
            g["rows"][0]["res"] != "gpu",
            not any(r["verdict"][0] <= EVICT for r in g["rows"]),
            g["rows"][0]["partition"],
            g["scope"]["account"] if g["scope"] else g["rows"][0]["account"],
        ),
    )


def table(headers, records):
    widths = [
        max([len(header)] + [vis_len(row[i]) for row in records])
        for i, header in enumerate(headers)
    ]
    lines = [
        "  " + C.DIM + "  ".join(pad(h, w) for h, w in zip(headers, widths)) + C.RESET,
        "  " + C.RULE + "  ".join("─" * w for w in widths) + C.RESET,
    ]
    lines.extend("  " + "  ".join(pad(cell, w) for cell, w in zip(row, widths)) for row in records)
    return lines


def render(model, args, width):
    L = []
    add = L.append
    rows = select(model, args)
    groups = pool_groups(rows)
    stamp = time.strftime("%a %d %b %H:%M:%S")
    title = "%sslurm accounts%s %s%s%s" % (C.BOLD, C.RESET, C.DIM, model["user"], C.RESET)
    add("")
    add(
        " "
        + title
        + " " * max(1, width - vis_len(title) - len(stamp) - 3)
        + C.DIM
        + stamp
        + C.RESET
    )

    ready_rows = [r for r in rows if r["has_normal"] and r["verdict"][0] <= EVICT]
    best = ready_rows[0] if ready_rows else None
    ready = sum(
        any(r["has_normal"] and r["verdict"][0] <= EVICT for r in g["rows"]) for g in groups
    )
    mine_r = sum(j["state"] == "R" for j in model["my_jobs"])
    mine_p = sum(j["state"] == "PD" for j in model["my_jobs"])
    headline = "%s %s" % (fmt_n(best["verdict"][4]), UNIT[best["res"]]) if best else "none"
    tiles = [
        ("best account offer", headline, C.OK if best else C.WARN),
        ("pools ready", "%d / %d" % (ready, len(groups)), C.INK),
        ("your jobs", "%d run · %d pending" % (mine_r, mine_p), C.INK),
    ]
    add("")
    labels, values = "  ", "  "
    for name, value, col in tiles:
        w = max(len(name), vis_len(value)) + 4
        labels += C.DIM + pad(name, w) + C.RESET
        values += col + C.BOLD + pad(value, w) + C.RESET
    add(values.rstrip())
    add(labels.rstrip())
    if best:
        add("")
        add("  %s→ %s%s%s" % (C.OK, C.BOLD, best["verdict"][2], C.RESET))
        add(
            "    %ssbatch --account=%s --partition=%s%s"
            % (C.MUTED, best["account"], best["partition"], C.RESET)
        )

    shared = {}
    for row in rows:
        for scope in row["shared_limits"]:
            shared[(scope["account"], scope["partition"])] = scope
    if shared:
        add("")
        add(rule("SHARED POOLS", width))
        records = []
        for (account, _partition), scope in sorted(
            shared.items(),
            key=lambda item: (
                not model["parts"].get(item[1]["partition"], {}).get("is_gpu", False),
                item[1]["partition"] or "",
                item[0],
            ),
        ):
            part = scope["partition"] or "all"
            name = account_label(
                account, part, pool=True, full_names=getattr(args, "full_names", False)
            )
            res = "gpu" if model["parts"].get(part, {}).get("is_gpu") else "cpu"
            members = [
                r
                for r in rows
                if r["partition"] == part
                and account in account_chain(r["account"], model["parents"])
            ]
            gpu_display = pool_gpu_display(model, scope, members) if res == "gpu" else None
            if gpu_display is not None:
                limit, remaining, free = gpu_display
                records.append(
                    [
                        C.INK + name + C.RESET,
                        part,
                        "%s GPU" % fmt_n(scope["used"].get("gpu", 0)),
                        limit,
                        (C.OK if free else C.WARN) + remaining + C.RESET,
                    ]
                )
                continue
            for key in ("node", "gpu", "cpu", "mem"):
                cap = scope["caps"].get(key)
                if cap is None:
                    continue
                col = C.OK if cap["free"] else C.WARN
                records.append(
                    [
                        C.INK + name + C.RESET,
                        part,
                        "%s %s" % (fmt_n(scope["used"].get(res, 0)), UNIT[res]),
                        "%s/%s %s" % (fmt_n(cap["used"]), fmt_n(cap["cap"]), UNIT[key]),
                        col + C.BOLD + "%s %s" % (fmt_n(cap["free"]), UNIT[key]) + C.RESET,
                    ]
                )
        L.extend(
            table(["POOL", "PARTITION", "IN USE", "USED / SHARED LIMIT", "EST. FREE"], records)
        )

    add("")
    add(rule("ACCOUNTS", width))
    records = []
    show_nodes = width >= 100 and any("node" in r["own_caps"] for r in rows)
    for group in groups:
        for row in group["rows"]:
            cap_key = row["primary"] or next(
                (k for k in ("cpu", "node", "mem") if k in row["own_caps"]), None
            )
            if cap_key:
                cap = row["own_caps"][cap_key]
                own = "%s%s/%s %s%s" % (
                    heat(cap["frac"]),
                    fmt_n(cap["used"]),
                    fmt_n(cap["cap"]),
                    UNIT[cap_key],
                    C.RESET,
                )
            else:
                own = C.DIM + "none" + C.RESET
            rank, glyph, label, color, offer = row["verdict"]
            if row["unknown_usage"]:
                offer_s = C.WARN + "unknown" + C.RESET
            elif offer:
                suffix = (
                    " p"
                    if not row["has_normal"]
                    else (" q" if rank == TIGHT else (" e" if rank == EVICT else ""))
                )
                offer_s = color + "%s %s%s" % (fmt_n(offer), UNIT[row["res"]], suffix) + C.RESET
            elif rank == PREEMPTONLY and row["phys_free"] >= 1:
                offer_s = C.WARN + "borrow p" + C.RESET
            else:
                offer_s = C.DIM + "0" + C.RESET
            queued = row["pending"]["jobs"]
            mine_r, mine_p = row["mine_run"]["jobs"], row["mine_pend"]["jobs"]
            mine = "%d%s" % (mine_r, "+%d" % mine_p if mine_p else "") if mine_r or mine_p else "·"
            cells = [C.MUTED + row["account"] + C.RESET, own]
            if show_nodes:
                nc = row["own_caps"].get("node")
                cells.append("%s/%s" % (fmt_n(nc["used"]), fmt_n(nc["cap"])) if nc else "·")
            cells.extend([offer_s, str(queued) if queued else "·", mine])
            records.append(cells)
    headers = ["ACCOUNT", "USED / OWN LIMIT"]
    if show_nodes:
        headers.append("OWN NODES")
    headers.extend(["EST. OFFER", "QUEUE", "YOURS"])
    if records:
        L.extend(table(headers, records))
        add(C.DIM + "  Offers include shared limits; do not add them across accounts." + C.RESET)
        add(
            C.DIM + "  At the node ceiling, jobs may still fit on already occupied nodes." + C.RESET
        )
        markers = []
        if any(" p" in cell for record in records for cell in record):
            markers.append("p = preemptable")
        if any(r["verdict"][0] == EVICT for r in rows):
            markers.append("e = evicts preemptable")
        if any(r["has_normal"] and r["verdict"][0] == TIGHT and r["verdict"][4] for r in rows):
            markers.append("q = queued demand")
        if markers:
            add(C.DIM + "  " + " · ".join(markers) + C.RESET)
    else:
        add(C.DIM + "  (nothing matches)" + C.RESET)

    partitions = sorted(
        {r["partition"] for r in rows}, key=lambda name: (not model["parts"][name]["is_gpu"], name)
    )
    if partitions:
        add("")
        add(rule("CLUSTER HARDWARE", width))
        records = []
        for name in partitions:
            p = model["parts"][name]
            res = "gpu" if p["is_gpu"] else "cpu"
            records.append(
                [
                    C.MUTED + name + C.RESET,
                    p["gpu_desc"] or "cpu",
                    "%s/%s %s" % (fmt_n(p[res + "_used"]), fmt_n(p[res]), UNIT[res]),
                    C.MUTED + "%s %s" % (fmt_n(p[res] - p[res + "_used"]), UNIT[res]) + C.RESET,
                ]
            )
        L.extend(table(["PARTITION", "DEVICE", "USED / ONLINE", "IDLE"], records))
        add(
            C.DIM
            + "  Cluster idle capacity is shared by all pools; filler jobs count as idle."
            + C.RESET
        )

    # ---- my jobs ----------------------------------------------------------
    if model["my_jobs"] and not args.no_jobs:
        add("")
        add(rule("YOUR JOBS", width))
        jobs = sorted(model["my_jobs"], key=lambda j: (j["state"] != "R", j["id"]))
        wa = max([len(j["account"]) for j in jobs] + [7])
        wn = min(30, max([len(j["name"]) for j in jobs] + [4]))
        for j in jobs[: args.max_jobs]:
            g = (
                (fmt_n(j["tres"]["gpu"]) + "g")
                if j["tres"]["gpu"]
                else (fmt_n(j["tres"]["cpu"]) + "c")
            )
            if j["state"] == "R":
                st, tail = C.OK + "R" + C.RESET, C.DIM + "%s left" % j["left"] + C.RESET
            else:
                st, tail = C.WARN + "PD" + C.RESET, C.MUTED + j["reason"] + C.RESET
            add(
                "  %s %s %s %s %s %s"
                % (
                    pad(C.DIM + j["id"] + C.RESET, 9),
                    pad(st, 3),
                    pad(C.INK + j["name"][:wn] + C.RESET, wn),
                    pad(C.MUTED + j["account"] + C.RESET, wa),
                    pad(g, 5),
                    tail,
                )
            )
        if len(jobs) > args.max_jobs:
            add("  " + C.DIM + "… %d more" % (len(jobs) - args.max_jobs) + C.RESET)

    add("")
    return "\n".join(L)


def rule(label, width):
    head = "%s%s%s " % (C.BOLD + C.ACCENT, label, C.RESET)
    return " " + head + C.RULE + "─" * max(0, width - vis_len(head) - 2) + C.RESET


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def to_json(model, args):
    out = {
        "user": model["user"],
        "cluster": model["cluster"],
        "generated": time.time(),
        "accounts": [],
        "partitions": [],
    }
    rows = select(model, args)
    shared = {
        (scope["account"], scope["partition"]): scope for r in rows for scope in r["shared_limits"]
    }
    out["shared_limits"] = [shared[a] for a in sorted(shared)]
    for r in rows:
        out["accounts"].append(
            {
                "account": r["account"],
                "partition": r["partition"],
                "qos": r["qos"],
                "primary_resource": r["res"],
                "quota": dict((k, v["cap"]) for k, v in r["caps"].items()),
                "used": dict((k, v["used"]) for k, v in r["caps"].items()),
                "free": dict((k, v["free"]) for k, v in r["caps"].items()),
                "own_quota": dict((k, v["cap"]) for k, v in r["own_caps"].items()),
                "own_used": dict((k, v["used"]) for k, v in r["own_caps"].items()),
                "partition_used": r["usage"],
                "limit_sources": dict((k, v["source"]) for k, v in r["caps"].items()),
                "shared_limits": r["shared_limits"],
                "unknown_usage": r["unknown_usage"],
                "offer_now": r["verdict"][4],
                "offer_now_nodes": r["offer_nodes"],
                "used_by_preemptable": r["preempt_used"],
                "pending_jobs_in_account": r["pending"]["jobs"],
                "partition_free": r["phys_free"],
                "partition_reclaimable": r["reclaim"],
                "my_running": r["mine_run"]["jobs"],
                "my_pending": r["mine_pend"]["jobs"],
                "status": r["verdict"][2],
                "rank": r["verdict"][0],
            }
        )
    for p in sorted(model["parts"].values(), key=lambda x: x["name"]):
        res = "gpu" if p["is_gpu"] else "cpu"
        out["partitions"].append(
            {
                "partition": p["name"],
                "device": p["gpu_desc"] or "cpu",
                "nodes_up": p["node"],
                "nodes_down": p["nodes_down"],
                "gpus_up": p["gpu"],
                "gpus_used": p["gpu_used"],
                "gpus_configured": p["gpu_cfg"],
                "cpus_up": p["cpu"],
                "cpus_used": p["cpu_used"],
                "cpus_configured": p["cpu_cfg"],
                "gpu_models": dict(p["models"]),
                "preemptable_held": model["part_preempt"][p["name"]][res],
                "filler_held": model["part_filler"][p["name"]][res],
                "pending_demand": model["part_pend"][p["name"]][res],
                "quota_booked": p["booked_" + res],
                "booked_ratio": (p["booked_" + res] / p[res + "_cfg"]) if p[res + "_cfg"] else None,
                "open_nodes": p["free_nodes"],
            }
        )
    return json.dumps(out, indent=2, sort_keys=True)


def pool_key(group):
    row, scope = group["rows"][0], group["scope"]
    return (row["partition"], scope["account"] if scope else row["account"])


def gpu_capacity(model, account, partition):
    """GPU ceiling constrained by the account tree, not just its parent node limit."""
    density = model["parts"][partition]["density"]
    children = model.get("_capacity_children")
    if children is None:
        children = defaultdict(list)
        for child, parent in model["parents"].items():
            children[parent].append(child)
        model["_capacity_children"] = children
    cache = model.setdefault("_gpu_capacity", {})

    def own_limit(source):
        limits = model["account_limits"].get(
            (source, partition), model["account_limits"].get((source, ""), {})
        )
        densities = [
            model["parts"][p]["density"]
            for p in model["account_partitions"].get(source, [partition])
            if p in model["parts"] and model["parts"][p].get("is_gpu", True)
        ]
        source_density = max(densities, default=density)
        return min(
            limits.get("gpu", float("inf")), limits.get("node", float("inf")) * source_density
        )

    def subtree(source):
        key = (partition, source)
        if key not in cache:
            descendants = children[source]
            child_capacity = (
                sum(subtree(child) for child in descendants) if descendants else float("inf")
            )
            cache[key] = min(own_limit(source), child_capacity)
        return cache[key]

    capacity = min(
        [subtree(account)]
        + [own_limit(parent) for parent in account_chain(account, model["parents"])[1:]]
    )
    return None if capacity == float("inf") else capacity


def fmt_nodes(value):
    return ("%.2f" % value).rstrip("0").rstrip(".")


def gpu_node_display(model, account, partition, used):
    capacity = gpu_capacity(model, account, partition)
    if capacity is None:
        return None
    density = model["parts"][partition]["density"]
    free = max(0, capacity - used) / density
    return (
        "%s/%s node" % (fmt_nodes(used / density), fmt_nodes(capacity / density)),
        "%s node" % fmt_nodes(free),
        free,
    )


def pool_gpu_display(model, scope, members):
    """Keep quota equivalents separate from estimated offers through attached accounts."""
    display = gpu_node_display(
        model,
        scope["account"],
        scope["partition"],
        scope.get("quota_used", scope["used"]).get("gpu", 0),
    )
    if display is None:
        return None
    limit, _, headroom = display
    if any(row["unknown_usage"] for row in members):
        return limit, "unknown", 0
    offer = max((row["verdict"][4] for row in members), default=0)
    available = min(headroom * model["parts"][scope["partition"]]["density"], offer)
    return limit, "%s GPU" % fmt_n(available), available


def interactive_rows(model, args, expanded):
    entries = []
    for group in pool_groups(select(model, args)):
        part, account = pool_key(group)
        scope, members = group["scope"], group["rows"]
        res = members[0]["res"]
        if scope:
            name = account_label(
                account, part, pool=True, full_names=getattr(args, "full_names", False)
            )
            key = (part, account)
            caps = scope["caps"]
            dimension = next(
                (k for k in ("node", "gpu", "cpu", "mem") if k in caps and caps[k]["free"] == 0),
                None,
            )
            dimension = dimension or next(k for k in ("node", "gpu", "cpu", "mem") if k in caps)
            cap = caps[dimension]
            limit_display = "%s/%s %s" % (fmt_n(cap["used"]), fmt_n(cap["cap"]), UNIT[dimension])
            remaining_display = "%s %s" % (fmt_n(cap["free"]), UNIT[dimension])
            available = cap["free"]
            gpu_display = pool_gpu_display(model, scope, members) if res == "gpu" else None
            if gpu_display is not None:
                limit_display, remaining_display, available = gpu_display
            my_run = sum(r["mine_run"]["jobs"] for r in members)
            my_pending = sum(r["mine_pend"]["jobs"] for r in members)
            entries.append(
                {
                    "key": key,
                    "parent": None,
                    "expandable": True,
                    "cells": [
                        ("▾ " if key in expanded else "▸ ") + name,
                        part,
                        "%s %s" % (fmt_n(scope["used"].get(res, 0)), UNIT[res]),
                        limit_display,
                        remaining_display,
                        "·",
                        "%d/%d" % (my_run, my_pending),
                    ],
                    "available": available,
                    "detail": "%s · %d attached account%s"
                    % (account, len(members), "" if len(members) == 1 else "s")
                    + (
                        " · quota shared across " + ", ".join(scope["shared_partitions"])
                        if len(scope["shared_partitions"]) > 1
                        else ""
                    ),
                    "limits": " · ".join(
                        "%s: %s/%s %s" % (s["account"], fmt_n(c["used"]), fmt_n(c["cap"]), UNIT[k])
                        for s in {
                            s["account"]: s for r in members for s in r["shared_limits"]
                        }.values()
                        for k, c in s["caps"].items()
                    ),
                }
            )
            if key not in expanded:
                continue
        for i, row in enumerate(members):
            key = (part, row["account"])
            name = account_label(
                row["account"],
                part,
                parent=account if scope else None,
                full_names=getattr(args, "full_names", False),
            )
            if scope:
                name = ("  └─ " if i == len(members) - 1 else "  ├─ ") + name
            else:
                name = "  " + name
            own = row["own_caps"]
            dimension = row["primary"] or next(
                (k for k in ("cpu", "node", "mem") if k in own), None
            )
            cap = own.get(dimension)
            limit = (
                "%s/%s %s" % (fmt_n(cap["used"]), fmt_n(cap["cap"]), UNIT[dimension])
                if cap
                else "none"
            )
            if res == "gpu":
                gpu_display = gpu_node_display(
                    model, row["account"], part, row["quota_usage"].get("gpu", 0)
                )
                if gpu_display is not None:
                    limit = gpu_display[0]
            rank, _, status, _, offer = row["verdict"]
            remaining = "%s %s" % (fmt_n(offer), UNIT[res])
            if row["unknown_usage"]:
                remaining = "unknown"
            elif offer:
                remaining += (
                    " p"
                    if not row["has_normal"]
                    else (" q" if rank == TIGHT else (" e" if rank == EVICT else ""))
                )
            elif rank == PREEMPTONLY and row["phys_free"] >= 1:
                remaining = "borrow p"
            entries.append(
                {
                    "key": key,
                    "parent": (part, account) if scope else None,
                    "expandable": False,
                    "cells": [
                        name,
                        part,
                        "%s %s" % (fmt_n(row["usage"].get(res, 0)), UNIT[res]),
                        limit,
                        remaining,
                        str(row["pending"]["jobs"]),
                        "%d/%d" % (row["mine_run"]["jobs"], row["mine_pend"]["jobs"]),
                    ],
                    "available": offer,
                    "detail": "%s · %s" % (row["account"], status),
                    "limits": "Own limits: "
                    + (
                        " · ".join(
                            "%s/%s %s" % (fmt_n(c["used"]), fmt_n(c["cap"]), UNIT[k])
                            for k, c in own.items()
                        )
                        or "none"
                    ),
                }
            )
        if scope:
            other_key = ("other", part, account, tuple(r["account"] for r in members))
            other_used = max(
                0, scope["used"].get(res, 0) - sum(r["usage"].get(res, 0) for r in members)
            )
            other_jobs = running_job_rows(model, other_key)
            if other_used or other_jobs:
                entries.append(
                    {
                        "key": other_key,
                        "parent": (part, account),
                        "expandable": False,
                        "cells": [
                            "  └─ Other accounts",
                            part,
                            "%s %s" % (fmt_n(other_used), UNIT[res]),
                            "shared",
                            "·",
                            "·",
                            "·",
                        ],
                        "available": 0,
                        "detail": "Other descendants of %s (not listed above)" % account,
                        "limits": "%d running jobs · Enter to expand" % len(other_jobs),
                    }
                )
    return entries


def expand_hosts(expression):
    """Expand Slurm node lists, including padded ranges and multiple bracket groups."""
    for item in re.split(r",(?![^\[]*\])", expression):
        if not item or item == "(null)":
            continue
        match = re.search(r"\[([0-9,-]+)\]", item)
        if not match:
            yield item
            continue
        for choice in match[1].split(","):
            first, separator, last = choice.partition("-")
            width = len(first) if first.startswith("0") else 0
            for number in range(int(first), int(last if separator else first) + 1):
                host = item[: match.start()] + str(number).zfill(width) + item[match.end() :]
                yield from expand_hosts(host)


def node_equivalent(job, nodes):
    """Estimate node equivalents from the largest allocated GPU, CPU, or RAM share."""
    hosts = list(expand_hosts(job["nodes"]))
    if not hosts or any(host not in nodes for host in hosts):
        return None
    fractions = {}
    for resource in ("gpu", "cpu", "mem"):
        allocated = job["tres"][resource]
        capacity = sum(nodes[host][resource] for host in hosts)
        if allocated and not capacity:
            return None
        fractions[resource] = allocated * len(hosts) / capacity if capacity else 0.0
    value = max(fractions.values())
    names = {"gpu": "GPU", "cpu": "CPU", "mem": "RAM"}
    bound = (
        "+".join(
            names[k] for k, fraction in fractions.items() if value and abs(fraction - value) < 1e-9
        )
        or "·"
    )
    return {"value": value, "bound": bound, "fractions": fractions, "nodes": len(hosts)}


def running_job_rows(model, selection):
    """Select an exact partition and an account subtree, including every user."""
    if selection[0] == "other":
        _, partition, account, excluded = selection
    else:
        partition, account = selection
        excluded = ()
    children = defaultdict(list)
    for child, parent in model["parents"].items():
        children[parent].append(child)
    accounts, pending = set(), [account]
    while pending:
        current = pending.pop()
        if current in accounts:
            continue
        accounts.add(current)
        pending.extend(children[current])
    jobs = [
        job
        for job in model["running_jobs"]
        if job["partition"] == partition
        and job["account"] in accounts
        and job["account"] not in excluded
    ]
    jobs.sort(key=lambda job: (job["user"], job["name"], job["id"]))
    entries = []
    for job in jobs:
        estimate = node_equivalent(job, model["node_resources"])
        node_eq = "%.2f" % estimate["value"] if estimate else "?"
        bound = estimate["bound"] if estimate else "?"
        fractions = (
            (
                "GPU %.2f · CPU %.2f · RAM %.2f node eq"
                % tuple(estimate["fractions"][k] for k in ("gpu", "cpu", "mem"))
            )
            if estimate
            else "Node estimate unavailable"
        )
        entries.append(
            {
                "key": ("job", job["id"]),
                "parent": None,
                "expandable": False,
                "cells": [
                    job["id"],
                    job["name"],
                    job["user"],
                    job["elapsed"],
                    job["limit"],
                    fmt_n(job["tres"]["gpu"]),
                    node_eq,
                    bound,
                ],
                "available": 0,
                "gpus": job["tres"]["gpu"],
                "node_estimate": estimate,
                "detail": "Name: " + job["name"],
                "limits": fractions,
                "identity": "%s · %s · QOS %s" % (job["id"], job["account"], job["qos"]),
            }
        )
    return entries


def inline_rows(model, args, expanded, expanded_jobs):
    """Insert job details beneath their owning row without replacing the pool table."""
    entries = []
    sources = interactive_rows(model, args, expanded)
    last_child = {r["parent"]: r["key"] for r in sources if r["parent"]}
    for source in sources:
        row = dict(source, kind="account", cells=list(source["cells"]))
        branch = ""
        if row["parent"]:
            branch = "  " if last_child[row["parent"]] == row["key"] else "│ "
        if not row["expandable"]:
            name = row["cells"][0][5:] if row["parent"] else row["cells"][0].strip()
            connector = ("└─" if branch == "  " else "├─") if row["parent"] else ""
            row["cells"][0] = connector + ("▾ " if row["key"] in expanded_jobs else "▸ ") + name
        entries.append(row)
        if row["key"] not in expanded_jobs:
            continue
        jobs = running_job_rows(model, row["key"])
        indent = 8 if row["parent"] else 6
        job_branch = "│ " if row["key"] in last_child else "  "
        header_tree = branch + ("├─  " if job_branch == "│ " else "└─  ")
        job_tree = branch + job_branch
        summary = "%d running jobs · %s GPUs allocated · all users" % (
            len(jobs),
            fmt_n(sum(job["gpus"] for job in jobs)),
        )
        entries.append(
            {
                "key": ("job_header",) + row["key"],
                "parent": row["key"],
                "kind": "job_header",
                "expandable": False,
                "indent": indent,
                "tree": header_tree,
                "cells": [
                    "▾ JOBS / ID",
                    "NAME",
                    "USER",
                    "ELAPSED",
                    "LIMIT",
                    "GPUs",
                    "NODE EQ",
                    "BOUND",
                ],
                "detail": summary,
                "limits": row["detail"],
            }
        )
        if not jobs:
            entries.append(
                {
                    "key": ("job_empty",) + row["key"],
                    "parent": row["key"],
                    "kind": "job_empty",
                    "expandable": False,
                    "indent": indent,
                    "tree": job_tree + "└─",
                    "cells": ["No running jobs."],
                    "detail": summary,
                    "limits": row["detail"],
                }
            )
        for i, job in enumerate(jobs):
            tree = job_tree + ("└─" if i == len(jobs) - 1 else "├─")
            entries.append(
                dict(
                    job,
                    kind="job",
                    indent=indent,
                    tree=tree,
                    parent=row["key"],
                    key=("job",) + row["key"] + (job["key"][1],),
                    detail=job["detail"],
                    limits=job["limits"] + " · " + summary,
                )
            )
    return entries


def column_widths(headers, rows, space):
    widths = [max([len(h)] + [len(r["cells"][i]) for r in rows]) for i, h in enumerate(headers)]
    while sum(widths) + 2 * (len(widths) - 1) > space:
        widest = max(range(len(widths)), key=lambda i: widths[i])
        widths[widest] -= 1
    return widths


def interactive(args):
    import curses
    import queue
    import threading

    def screen(stdscr):
        # Cursor hiding is optional on terminals without the corresponding capability.
        try:
            curses.curs_set(0)
        except curses.error:
            pass
        stdscr.keypad(True)
        stdscr.timeout(100)
        curses.mouseinterval(0)
        wheel_down = getattr(curses, "BUTTON5_PRESSED", 0)
        curses.mousemask(curses.BUTTON1_PRESSED | curses.BUTTON4_PRESSED | wheel_down)
        color = not (args.no_color or os.environ.get("NO_COLOR")) and curses.has_colors()
        if color:
            curses.start_color()
            background = curses.COLOR_BLACK
            try:
                curses.use_default_colors()
                background = -1
            except curses.error:
                pass
            for pair, foreground in (
                (1, curses.COLOR_CYAN),
                (2, curses.COLOR_GREEN),
                (3, curses.COLOR_YELLOW),
            ):
                curses.init_pair(pair, foreground, background)

        updates = queue.Queue()
        model, error, busy = None, "", False
        expanded = set()
        expanded_jobs = set()
        selected, offset, selected_key = 0, 0, None
        next_refresh, stamp = 0, ""
        interval = max(2, args.watch if args.watch is not None else 20)

        def refresh():
            def fetch():
                try:
                    ignore = tuple(u for u in args.ignore_users.split(",") if u)
                    result = load_model(args, ignore)
                except (Exception, SystemExit) as exc:
                    updates.put((None, str(exc)))
                else:
                    updates.put((result, ""))

            threading.Thread(target=fetch, daemon=True).start()

        def put(y, x, text, attr=0):
            height, columns = stdscr.getmaxyx()
            if 0 <= y < height and x < columns - 1:
                stdscr.addstr(y, x, text[: max(0, columns - x - 1)], attr)

        def clip(text, size):
            return text if len(text) <= size else text[: max(0, size - 1)] + "…"

        while True:
            if not updates.empty():
                fresh, error = updates.get_nowait()
                if fresh is not None:
                    model = fresh
                    stamp = time.strftime("%H:%M:%S")
                busy = False
                next_refresh = time.monotonic() + interval
            if not busy and time.monotonic() >= next_refresh:
                refresh()
                busy = True
            height, columns = stdscr.getmaxyx()
            entries = inline_rows(model, args, expanded, expanded_jobs) if model else []
            if selected_key is not None:
                selected = next(
                    (i for i, row in enumerate(entries) if row["key"] == selected_key), selected
                )
            selected = min(selected, max(0, len(entries) - 1))
            selected_key = entries[selected]["key"] if entries else None
            top, visible = 4, max(1, height - 10)
            offset = min(offset, max(0, len(entries) - visible))
            if selected < offset:
                offset = selected
            elif selected >= offset + visible:
                offset = selected - visible + 1
            stdscr.erase()
            put(0, 2, "SHARED POOLS", curses.A_BOLD | (curses.color_pair(1) if color else 0))
            state = "refreshing…" if busy else ("updated " + stamp if stamp else "")
            put(0, max(17, columns - len(state) - 2), state, curses.A_DIM)
            if columns < 60 or height < 12:
                put(2, 1, "Resize to at least 60 columns and 12 rows; q quits.")
                visible = 0
            else:
                headers = ["POOL / ACCOUNT", "PARTITION", "IN USE", "USED / LIMIT", "EST. FREE"]
                if columns >= 100:
                    headers += ["QUEUE", "MY R/P"]
                all_keys = (
                    {pool_key(g) for g in pool_groups(select(model, args))} if model else set()
                )
                all_rows = inline_rows(model, args, all_keys, expanded_jobs) if model else []
                account_rows = [r for r in all_rows if r["kind"] == "account"]
                widths = column_widths(headers, account_rows, columns - 4)
                job_rows = [r for r in entries if r["kind"] in ("job", "job_header")]
                job_headers = [
                    "▾ JOBS / ID",
                    "NAME",
                    "USER",
                    "ELAPSED",
                    "LIMIT",
                    "GPUs",
                    "NODE EQ",
                    "BOUND",
                ]
                job_columns = list(range(8)) if columns >= 100 else [1, 2, 3, 4, 5, 6]
                if columns < 100:
                    job_headers[1] = "▾ JOBS / NAME"
                job_widths = column_widths(
                    [job_headers[i] for i in job_columns],
                    [{"cells": [r["cells"][i] for i in job_columns]} for r in job_rows],
                    columns - 10,
                )
                x = 2
                for header, w in zip(headers, widths):
                    put(2, x, clip(header, w), curses.A_BOLD)
                    put(3, x, "─" * w, curses.A_DIM)
                    x += w + 2
                for index, row in enumerate(entries[offset : offset + visible], offset):
                    is_pool = row["kind"] == "account" and not row["parent"]
                    attr = curses.A_REVERSE if index == selected else 0
                    y = top + index - offset
                    put(y, 1, " " * (columns - 2), attr)
                    if "tree" in row:
                        put(y, 2, row["tree"], attr | curses.A_DIM)
                    if row["kind"] == "job_empty":
                        put(y, row["indent"], row["cells"][0], attr | curses.A_DIM)
                        continue
                    is_account = row["kind"] == "account"
                    row_widths = widths if is_account else job_widths
                    x = 2 if is_account else row["indent"]
                    if row["kind"] == "job_header":
                        attr |= curses.A_DIM
                    source_cells = job_headers if row["kind"] == "job_header" else row["cells"]
                    cells = source_cells if is_account else [source_cells[i] for i in job_columns]
                    for i, w in enumerate(row_widths):
                        field_attr = attr
                        if i == 0 and is_pool:
                            field_attr |= curses.A_BOLD
                        if i == 4 and color and is_account and index != selected:
                            field_attr |= curses.color_pair(2 if row["available"] else 3)
                        value = clip(cells[i], w).ljust(w)
                        if i == 0 and is_account and row["parent"]:
                            put(y, x, value[:2], attr | curses.A_DIM)
                            put(y, x + 2, value[2:], field_attr)
                        else:
                            put(y, x, value, field_attr)
                        x += w + 2
                if not entries:
                    put(
                        top,
                        2,
                        "Loading Slurm usage…" if busy else "No matching accounts.",
                        curses.A_DIM,
                    )
                if entries:
                    put(height - 5, 2, entries[selected]["detail"], curses.A_DIM)
                    put(height - 4, 2, entries[selected]["limits"], curses.A_DIM)
                put(
                    height - 3,
                    2,
                    entries[selected]["identity"]
                    if entries and entries[selected]["kind"] == "job"
                    else "",
                    curses.A_DIM,
                )
                put(
                    height - 2,
                    2,
                    "Enter jobs | Click/Space expand | Left collapse | r refresh | q quit",
                )
                note = (
                    "NODE EQ ≈ max(GPU, CPU, host RAM share); actual packing can differ."
                    if expanded_jobs
                    else ""
                )
                put(
                    height - 1,
                    2,
                    error or note,
                    curses.color_pair(3) if color and error else curses.A_DIM,
                )
            stdscr.refresh()
            key = stdscr.getch()
            if key in (ord("q"), ord("Q")):
                return
            if key == ord("r"):
                next_refresh = 0
            row = entries[selected] if entries else None
            if key == curses.KEY_MOUSE:
                try:
                    _, _, mouse_y, _, button = curses.getmouse()
                except curses.error:
                    continue
                if button & curses.BUTTON4_PRESSED:
                    selected = max(0, selected - 3)
                elif button & wheel_down:
                    selected = min(max(0, len(entries) - 1), selected + 3)
                elif button & curses.BUTTON1_PRESSED and top <= mouse_y < top + min(
                    visible, len(entries) - offset
                ):
                    selected = offset + mouse_y - top
                    row = entries[selected]
                    if row["kind"] == "account":
                        target = expanded if row["expandable"] else expanded_jobs
                        target.symmetric_difference_update({row["key"]})
                    elif row["kind"] == "job_header":
                        expanded_jobs.discard(row["parent"])
                        selected = next(
                            i for i, r in enumerate(entries) if r["key"] == row["parent"]
                        )
            elif key in (curses.KEY_DOWN, ord("j")):
                selected = min(max(0, len(entries) - 1), selected + 1)
            elif key in (curses.KEY_UP, ord("k")):
                selected = max(0, selected - 1)
            elif key == curses.KEY_NPAGE:
                selected = min(max(0, len(entries) - 1), selected + max(1, visible))
            elif key == curses.KEY_PPAGE:
                selected = max(0, selected - max(1, visible))
            elif key == curses.KEY_HOME:
                selected = 0
            elif key == curses.KEY_END:
                selected = max(0, len(entries) - 1)
            elif row and key in (10, 13, curses.KEY_ENTER):
                if row["kind"] == "account":
                    expanded_jobs.symmetric_difference_update({row["key"]})
                else:
                    expanded_jobs.discard(row["parent"])
                    selected = next(i for i, r in enumerate(entries) if r["key"] == row["parent"])
            elif row and key in (ord(" "), curses.KEY_RIGHT):
                if row["kind"] == "account":
                    target = expanded if row["expandable"] else expanded_jobs
                    if key == curses.KEY_RIGHT:
                        target.add(row["key"])
                    else:
                        target.symmetric_difference_update({row["key"]})
            elif row and key in (27, curses.KEY_LEFT, ord("b")):
                target = row["key"] if row["kind"] == "account" else row["parent"]
                if target in expanded_jobs:
                    expanded_jobs.discard(target)
                elif target in expanded:
                    expanded.discard(target)
                elif row["parent"]:
                    target = row["parent"]
                    expanded.discard(target)
                selected = next((i for i, r in enumerate(entries) if r["key"] == target), selected)
            selected_key = entries[selected]["key"] if entries else None

    try:
        curses.wrapper(screen)
    except KeyboardInterrupt:
        pass
    return 0


def load_model(args, ignore):
    raw = (
        json.loads(files("slurm_wtf").joinpath("demo.json").read_text())
        if args.demo
        else collect(args.user)
    )
    return build(
        raw,
        "demo" if args.demo else args.user,
        ignore,
        tuple(q.strip() for q in args.preemptible_qos.split(",") if q.strip()),
    )


def main(argv=None):
    ap = argparse.ArgumentParser(
        prog="wtf",
        description="Where did the cluster capacity go? Slurm accounts, shared limits, and running jobs.",
    )
    ap.add_argument("--version", action="version", version="%(prog)s " + __version__)
    ap.add_argument("--demo", action="store_true", help="explore synthetic data without Slurm")
    ap.add_argument(
        "--full-names", action="store_true", help="show exact account identifiers in table labels"
    )
    ap.add_argument(
        "--preemptible-qos",
        default=os.environ.get("SA_PREEMPTIBLE_QOS", ""),
        help="comma-separated QoS names treated as reclaimable (default: none)",
    )
    ap.add_argument(
        "-u",
        "--user",
        default=os.environ.get("USER") or getpass.getuser(),
        help="inspect another user's associations",
    )
    ap.add_argument(
        "-w",
        "--watch",
        nargs="?",
        const=20,
        type=int,
        metavar="SEC",
        help="refresh interval; also enables watching --plain output (default 20s)",
    )
    ap.add_argument("-g", "--gpu", dest="gpu_only", action="store_true", help="GPU partitions only")
    ap.add_argument(
        "-c", "--cpu", dest="cpu_only", action="store_true", help="CPU-only partitions only"
    )
    ap.add_argument(
        "-A",
        "--all",
        dest="all_accounts",
        action="store_true",
        help="also show accounts with no quota (preemptable-only)",
    )
    ap.add_argument("-m", "--match", metavar="REGEX", help="filter accounts/partitions")
    ap.add_argument("-J", "--no-jobs", action="store_true", help="hide your job list")
    ap.add_argument("--max-jobs", type=int, default=15, help="cap the job list (default 15)")
    ap.add_argument(
        "--ignore-users",
        default=os.environ.get("SA_IGNORE_USERS", ",".join(FILLER_USERS)),
        metavar="U1,U2",
        help="users whose jobs should count as reclaimable capacity "
        "(default: %s; env SA_IGNORE_USERS)" % ",".join(FILLER_USERS),
    )
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--no-color", action="store_true")
    ap.add_argument("--plain", action="store_true", help="print a noninteractive snapshot")
    args = ap.parse_args(argv)
    if args.match:
        try:
            re.compile(args.match)
        except re.error as exc:
            ap.error("invalid --match expression: " + str(exc))

    if args.no_color or os.environ.get("NO_COLOR") or (not args.json and not sys.stdout.isatty()):
        C.strip()

    if (
        not args.json
        and not args.plain
        and sys.stdin.isatty()
        and sys.stdout.isatty()
        and os.environ.get("TERM", "dumb") != "dumb"
    ):
        return interactive(args)

    def frame():
        ignore = tuple(u for u in args.ignore_users.split(",") if u)
        model = load_model(args, ignore)
        if args.json:
            return to_json(model, args)
        cols = shutil.get_terminal_size((104, 40)).columns
        return render(model, args, max(76, min(cols, 170)))

    if args.watch is None or args.json:
        print(frame())
        return 0

    interval = max(2, args.watch)
    sys.stdout.write("\033[?1049h\033[?25l")
    try:
        while True:
            body = frame()
            sys.stdout.write("\033[H\033[2J" + body)
            sys.stdout.write(
                "\n %srefreshing every %ds · ctrl-c to quit%s\n" % (C.DIM, interval, C.RESET)
            )
            sys.stdout.flush()
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        sys.stdout.write("\033[?25h\033[?1049l")
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
