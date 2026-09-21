# slurm-wtf

**Where did the cluster capacity go?**

`wtf` shows Slurm account usage, shared parent limits, estimated free capacity, and the jobs consuming it in one expandable terminal table.

## Install

```bash
uv tool install slurm-wtf
wtf
```

Install directly from GitHub:

```bash
uv tool install git+https://github.com/youngsm/slurm-wtf.git
```

Run without installing:

```bash
uvx --from slurm-wtf wtf --demo
```

Requires Python 3.10+ and a POSIX terminal.
Live mode also requires `sacctmgr`, `scontrol`, `squeue`, `sinfo`, and access to the cluster's accounting information.
The package has no Python runtime dependencies.

## Explore

```bash
wtf                 # interactive pools, accounts, and inline jobs
wtf --demo          # synthetic example, no cluster required
wtf -g              # GPU partitions
wtf -c              # CPU partitions
wtf -m research     # filter account or partition names
wtf --plain         # printable snapshot
wtf --json          # machine-readable snapshot
wtf -w 30           # refresh every 30 seconds
```

| Action | Control |
|---|---|
| Expand pool accounts | Click, Space, or Right |
| Expand running jobs | Enter on a pool or account |
| Collapse a branch | Left or Escape |
| Move | Arrows, j/k, Page Up/Down, mouse wheel |
| Favorite a pool/partition | x |
| Refresh / quit | r / q |

Press `x` on a pool to add or remove its star and pin it above other pools.
Favorites stay alphabetically ordered by partition, followed by the other pools in alphabetical order.
Pressing `x` within an expanded account or job favorites its enclosing pool.
Favorites are saved by cluster, partition, and full account identifier in `$XDG_CONFIG_HOME/slurm-wtf/favorites.json` (default `~/.config/slurm-wtf/favorites.json`).
The synthetic demo uses a separate favorites file.

Jobs expand in place, with name, user, elapsed time, time limit, GPUs, and estimated node share.
The selected job's footer shows its full name and account.
Redirected output is automatically noninteractive.
`--no-color` and `NO_COLOR` disable colors.

## Reading the numbers

- **IN USE:** resource usage on this partition, across all users in the account or pool.
- **QUOTA USAGE:** GPU-node equivalents for GPU partitions, respecting child GPU ceilings and shared ancestors.
- **EST. NOW:** a conservative estimate through an attached account, constrained by account quotas and idle plus configured reclaimable hardware. The selected row shows both idle and reclaimable counts for the whole partition.
- **NODE EQ:** a job's largest GPU, CPU, or host-RAM share of its assigned nodes.

Four GPUs on four-GPU hosts equal one GPU-node equivalent, even if the placement allowance permits using two separate hosts.
Shared child quotas are never added beyond the parent ceiling.
Pool estimates use the best attached-account offer; do not add pool and child estimates together.
Quota is a ceiling, not reserved hardware: a pool using 7 of 28 GPU equivalents can still have no idle hardware available. Configured filler jobs contribute reclaimable capacity, including when some GPUs are already idle. Node limits are separate from GPU-node equivalents and can further constrain the estimate.

## Cluster portability

Account names require no prefix, separator, partition suffix, or institution-specific convention.
Structured names are shortened for display, while full identifiers remain in selected-row details and JSON.
Use `--full-names` to show exact identifiers in the table; label formatting never controls access, quotas, or job matching.
Partitions come from Slurm associations and partition metadata.
An association without a partition restriction is expanded over accessible partitions, respecting published account, group, and QoS access lists.
Database queries are restricted to the current cluster and relevant shared pools, and controller counters retain partition scope.
CPU-only clusters and both typed and untyped GPU resources are supported.

All users and QoS names count as ordinary usage by default.
If your administrators confirm that certain jobs are reclaimable, configure that explicitly:

```bash
wtf --preemptible-qos scavenger,spot
wtf --ignore-users placeholder
```

`SA_PREEMPTIBLE_QOS` and `SA_IGNORE_USERS` provide the same comma-separated defaults.
No username or cluster policy is built in, and an unfamiliar QoS is never assumed reclaimable.

Save filler users once for the current Slurm cluster:

```bash
wtf --save-filler-users placeholder,background
wtf
wtf --save-filler-users ""  # clear the saved list for this cluster
```

Settings are stored by cluster name in `$XDG_CONFIG_HOME/slurm-wtf/settings.json`
(default `~/.config/slurm-wtf/settings.json`). `--ignore-users` overrides
`SA_IGNORE_USERS`, which overrides the saved setting. An explicit empty value
(`wtf --ignore-users ""`) disables filler treatment for that invocation.
Demo mode does not load these settings. Filler jobs remain visible in job lists.

Idle and reclaimable GPUs are reported separately; filler allocations are not
subtracted from the actual allocated GPU/CPU counts. In JSON, `partition_free`
and partition `open_nodes` describe truly idle hardware; `partition_reclaimable`
includes configured filler and preemptible QoS allocations, without double counting.
Account and shared limits still apply to the combined estimate.

## Limits of an estimate

This is an accounting view, not a replacement for Slurm's scheduler.
Reservations, user-specific limits, QoS resource caps, fair-share priority, job-submit plugins, and a particular job's CPU, RAM, GPU type, or placement constraints can reduce what can run.
Read permissions and `PrivateData` can restrict visibility; unavailable controller usage is shown as unknown.
Shared account limits may span partitions and should not be summed across their rows.
For such accounts, IN USE is derived from visible jobs on the selected partition, while QUOTA USAGE retains account-wide quota usage.
The selected pool identifies which partitions share its quota.
On heterogeneous partitions, GPU-node equivalents and job shares are estimates based on reported hardware capacities.

## Release notifications

Live interactive and plain output automatically check PyPI for a newer installable
stable release. The interactive check runs in the background. An up-to-date result
is cached for one hour, an available-version notice for 24 hours, and a failed
attempt for five minutes in `$XDG_CACHE_HOME/slurm-wtf/update-check.json` (default
`~/.cache/slurm-wtf/update-check.json`). Offline failures are silent. The checker
uses the host CA bundle when an isolated Python runtime cannot find it.
Demo and JSON output never trigger automatic checks. Plain snapshots print any
notice to stderr after the report, waiting at most two additional seconds.

```bash
wtf --check-updates        # force a fresh check and exit; no Slurm required
uv tool upgrade slurm-wtf  # update a uv-managed installation
wtf --no-update-check     # disable automatic checking for this invocation
```

Set `SLURM_WTF_NO_UPDATE_CHECK=1` to disable automatic checks persistently.
For pip installations, update with `python -m pip install --upgrade slurm-wtf`.
Checks only notify; they never install anything. Explicit checks report network
failures and return a nonzero exit status.

## Development

```bash
uv sync --all-extras
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run wtf --demo
uv build
```

Tests use synthetic Slurm snapshots and require no cluster access.
See [RELEASING.md](../RELEASING.md) for PyPI publishing.
