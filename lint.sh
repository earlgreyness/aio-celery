#!/bin/bash

set -ex

ruff format --check --diff --quiet .
ruff check --diff .
ruff check .
mypy .
find . -type f -name '*.sh' -exec shellcheck -x {} +
