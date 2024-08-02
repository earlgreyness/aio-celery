#!/bin/bash

set -eu

if [[ -z ${PYPI_TOKEN} ]]; then
    echo "Error: environment variable PYPI_TOKEN not set" >&2
    exit 1
fi

source ./venv/bin/activate

echo "Building aio_celery..."
rm dist/*
python3 -m build

echo "Uploading aio_celery to PyPI (pypi.org)..."
expect <<EOF
spawn python3 -m twine upload dist/*
expect "Enter your username: "
send "__token__\r"
expect "Enter your password: "
send "${PYPI_TOKEN}\n\r"
expect {
    "ERROR" {
        puts "\nError occured\n"
        exit 1
    }
    eof {

    }
}
EOF

echo "Success."
