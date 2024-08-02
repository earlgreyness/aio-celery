#!/bin/bash

set -ex

pip install --upgrade \
    mypy \
    ruff \
    types-psutil \
    types-redis

brew install \
    shellcheck \
    shfmt
