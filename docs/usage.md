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
- **USED / LIMIT:** GPU-node equivalents for GPU partitions, respecting child GPU ceilings and shared ancestors.
- **EST. FREE:** a conservative estimate through an attached account, constrained by account quotas and available hardware.
- **NODE EQ:** a job's largest GPU, CPU, or host-RAM share of its assigned nodes.

Four GPUs on four-GPU hosts equal one GPU-node equivalent, even if the placement allowance permits using two separate hosts.
Shared child quotas are never added beyond the parent ceiling.
Pool estimates use the best attached-account offer; do not add pool and child estimates together.
A zero estimate at an occupied-node limit does not rule out a smaller job fitting on an already occupied host.

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
No user is ignored automatically, and an unfamiliar QoS is never assumed reclaimable.

## Limits of an estimate

This is an accounting view, not a replacement for Slurm's scheduler.
Reservations, user-specific limits, QoS resource caps, fair-share priority, job-submit plugins, and a particular job's CPU, RAM, GPU type, or placement constraints can reduce what can run.
Read permissions and `PrivateData` can restrict visibility; unavailable controller usage is shown as unknown.
Shared account limits may span partitions and should not be summed across their rows.
For such accounts, IN USE is derived from visible jobs on the selected partition, while USED / LIMIT retains account-wide quota usage.
The selected pool identifies which partitions share its quota.
On heterogeneous partitions, GPU-node equivalents and job shares are estimates based on reported hardware capacities.

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
