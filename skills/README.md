# Agent skill

`slurm-job-placement` teaches Claude Code and Codex to use `wtf`'s live JSON output when recommending an account and partition for a Slurm job.

Install `slurm-wtf` first and make sure `wtf --json` works on the cluster login host. Then, from this repository's root, copy the skill into either agent's personal skill directory:

```bash
# Codex
mkdir -p ~/.agents/skills
cp -R skills/slurm-job-placement ~/.agents/skills/

# Claude Code
mkdir -p ~/.claude/skills
cp -R skills/slurm-job-placement ~/.claude/skills/
```

Install both copies if you use both agents. Remove the destination directory before copying again when updating an existing installation. Codex normally detects a new skill automatically; restart the agent if it does not appear. Claude Code discovers skills in `~/.claude/skills` automatically.

Invoke the skill by asking where to submit a Slurm job, or name it directly as `$slurm-job-placement`. For example:

> Use slurm-job-placement to find the best account and partition for this 4×A100 job, then show me the sbatch command.
