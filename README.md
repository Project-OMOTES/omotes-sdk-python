# OMOTES SDK Python

This repository is part of the 'Nieuwe Warmte Nu Design Toolkit' project.

Python implementation of the OMOTES SDK through jobs which may be submitted, receive status updates for submitted jobs or delete submitted jobs.

## Development

### Tools

This project uses:

- **uv**: Fast Python package manager and resolver. Install via [https://docs.astral.sh/uv/](https://docs.astral.sh/uv/)
- **just**: Command runner for common tasks (similar to Make). Install via [https://github.com/casey/just](https://github.com/casey/just)

### Setup

Install dependencies and update `uv.lock` file after updating `pyproject.toml` dependencies:

```bash
uv sync
```

### Lint/typecheck/test locally

Run via just (also used in github actions):

```bash
just ci            # run all CI checks (lint, format-check, typecheck, test)

just lint          # ruff checks
just security      # ruff security
just format        # ruff format
just format-check  # verify formatting
just typecheck     # ty type checking
just test          # pytest
```

To debug test go to the debug view in vscode and run "pytest".
