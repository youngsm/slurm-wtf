![slurm-wtf: expand shared pools, accounts, and running jobs](https://raw.githubusercontent.com/youngsm/slurm-wtf/main/docs/demo.gif)

```bash
pip install slurm-wtf
wtf             # live cluster usage
wtf --demo      # try it without a cluster
```

## Agent skill

The repository includes a [`slurm-job-placement`](skills/slurm-job-placement/SKILL.md) skill that lets Claude Code and Codex use `wtf` to recommend an account and partition for a job. See the [skill installation instructions](skills/README.md).
