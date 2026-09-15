# Releasing slurm-wtf

Publishing uses GitHub Actions and PyPI Trusted Publishing, with no long-lived token stored in the repository.

## First release

Add a pending publisher at https://pypi.org/manage/account/publishing/ with these values:

| Field | Value |
|---|---|
| PyPI project name | `slurm-wtf` |
| GitHub owner | `youngsm` |
| Repository | `slurm-wtf` |
| Workflow filename | `publish.yml` |
| Environment | `pypi` |

The PyPI account creating this pending publisher becomes an owner when the first upload succeeds.
Registering the pending publisher does not reserve the project name.

## Publish a version

1. Update the version in `pyproject.toml` and `src/slurm_wtf/__init__.py`.
2. Run `uv sync --all-extras`, `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`, and `uv build`.
3. Commit and push the version change, then tag the same commit as `vVERSION`.
4. Publish a GitHub release for that tag, or run the Publish to PyPI workflow on the tag.

The workflow checks the tag, runs the tests, builds the wheel and source archive, exercises the installed wheel with synthetic data, and uploads those artifacts to PyPI using the configured publisher.
A build failure prevents publishing.

To verify installation after publishing:

```bash
uvx --from slurm-wtf wtf --version
uvx --from slurm-wtf wtf --demo
```
