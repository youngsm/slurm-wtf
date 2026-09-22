---
name: slurm-job-placement
description: Use slurm-wtf's live JSON snapshot to recommend a Slurm account and partition for a job. Use when deciding where to submit GPU or CPU work, comparing currently available capacity, checking whether a batch script fits, or producing appropriate sbatch account and partition flags.
---

# Slurm job placement

Recommend a submission target from current scheduler state. Treat the result as a snapshot, because availability can change before submission.

This workflow requires `wtf` on a host with access to the target Slurm cluster.

## Gather the request

Read an existing batch script when the user provides one. Extract its `#SBATCH` account, partition, GPU or CPU count, GPU type, node count, CPUs, memory, time limit, and QoS constraints.

Otherwise, use requirements already stated by the user. Ask only for a missing constraint that could change the recommendation. If the user merely asks where capacity exists, assume one unit of the requested primary resource and state that assumption.

## Get live capacity

Check that `wtf` is available, then request machine-readable output:

```bash
command -v wtf
wtf --json -g   # GPU work
wtf --json -c   # CPU-only work
```

Use `wtf --json` without a resource filter when comparing both. Add `-m REGEX` only when the user has already constrained the account or partition. Do not parse the colored or interactive display.

If `wtf` is missing, explain that the user can install it with `uv tool install slurm-wtf` or `python -m pip install --user slurm-wtf`. Do not install software unless the user asks.

If live Slurm queries fail, report the error and stop. Do not invent availability from account names, static documentation, or an old snapshot.

## Choose a target

Join each account row to its partition row by the `partition` field, then apply these rules:

1. Reject rows whose `primary_resource` does not match the job, whose `unknown_usage` is nonempty, or whose `offer_now` is smaller than the requested GPU or CPU count.
2. For GPU work, reject partitions whose `gpu_models` cannot satisfy an explicitly requested model. Treat the model names reported by Slurm as cluster-defined labels; do not guess that two labels are equivalent.
3. When the job requests whole nodes, require `offer_now_nodes` to meet the node count. `gpu_models` reports partition totals, so do not use it to infer exact GPUs per node; call out topology as an unresolved scheduler constraint when it matters.
4. Prefer capacity covered by `partition_free`. Capacity beyond that value and up to `partition_free + partition_reclaimable` may require reclaiming configured filler or preemptible jobs; label this clearly.
5. Among fitting rows, prefer lower `pending_jobs_in_account`, lower partition `pending_demand`, and then more headroom in `offer_now`. Use `status` to explain quota or shared-limit pressure.
6. Treat accounts connected by the same entry in `shared_limits` as alternate ways into one shared pool. Recommend one of them; never add their offers together.
7. Use `open_nodes` only as supporting evidence. It is a fractional estimate and does not prove that a job's exact CPU, memory, GPU topology, or wall-time request fits.

If no row fits, say which constraint blocks the request and show the nearest candidates. Do not recommend splitting one job across unrelated accounts or partitions unless the workload itself can be split and the user asks for that option.

## Present the recommendation

Lead with the best account and partition and include:

- requested resources and any assumptions;
- `offer_now`, including how much is truly idle versus reclaimable;
- GPU model for GPU work;
- queued demand or shared-limit pressure that affected the choice;
- one fallback when a credible alternative exists.

Provide a submission fragment without fabricating unspecified resource flags:

```bash
sbatch --account=<account> --partition=<partition> <existing job flags> job.sh
```

Do not submit the job unless the user explicitly asks. When they do, refresh `wtf --json` immediately before submission and preserve all requirements from their script or command.
