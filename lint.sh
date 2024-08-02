#!/bin/bash

set -ex

ruff format --check --diff --quiet .
ruff check --diff .
ruff check .
mypy .
find . -type f -name '*.sh' -not -path "./venv/*" -exec shellcheck -x {} +
find . -type f -name '*.sh' -not -path "./venv/*" -exec shfmt -d -i 4 -ci -bn -s {} +
