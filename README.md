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
just ci            # run all CI checks (lint, security, format-check, typecheck, test)

just lint          # ruff checks
just security      # ruff security
just format        # ruff format
just format-check  # verify formatting
just typecheck     # ty type checking
just test          # pytest
```

To debug test go to the debug view in vscode and run "pytest".

## Project Structure

```text
omotes-sdk-python/
├── src/
│   ├── omotes_sdk/
│   │   ├── __init__.py          # Package exports and version metadata
│   │   ├── esdl_messages.py     # ESDL-related message models/utilities
│   │   ├── job_status.py        # Job status enum and mappings
│   │   ├── log_forwarding.py    # Logging forwarder and queue handling
│   │   ├── memory_quantity.py   # Memory quantity parsing/conversion helpers
│   │   └── prefect_util.py      # Prefect deployment/run/status helper functions
│   └── omotes_sdk_python.egg-info/
│       ├── PKG-INFO
│       ├── SOURCES.txt
│       ├── dependency_links.txt
│       ├── requires.txt
│       └── top_level.txt
├── tests/
│   ├── test_log_forwarding.py   # Tests for log forwarding behavior
│   └── test_prefect_util.py     # Tests for Prefect utility behavior
├── justfile                     # Task runner commands
├── pyproject.toml               # Project metadata and dependencies
├── CHANGELOG.md                 # Release notes
├── CONTRIBUTING.md              # Contribution guidelines
├── LICENSE
└── README.md
```
