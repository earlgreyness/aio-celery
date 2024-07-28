#!/bin/bash

set -ex

ruff format .
ruff check --fix-only .
ruff format .
