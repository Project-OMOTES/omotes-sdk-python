#!/usr/bin/env bash

if [[ "$OSTYPE" != "win32" && "$OSTYPE" != "msys" ]]; then
  echo "Activating .venv first."
  . .venv/bin/activate
fi

pip-compile -U --extra=dev --output-file=requirements.txt pyproject.toml
pip install -r ./requirements.txt
