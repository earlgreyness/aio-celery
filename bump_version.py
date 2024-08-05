#!/usr/bin/env python3

from __future__ import annotations

import pathlib
import re
import subprocess  # noqa: S404


def fork(*command: str) -> None:
    subprocess.run(command, check=True)  # noqa: S603


def main() -> None:
    file = pathlib.Path("./aio_celery/__init__.py")
    content = file.read_text(encoding="utf8")
    pattern = re.compile(r"__version__ = \"(\d+)\.(\d+)\.(\d+)\"")

    version = pattern.search(content)
    if version is None:
        raise RuntimeError
    major = int(version[1])
    minor = int(version[2])
    patch = int(version[3])

    new_version = f"{major}.{minor + 1}.{patch}"

    file.write_text(pattern.sub(f'__version__ = "{new_version}"', content))
    fork("/usr/bin/git", "commit", "-m", f"Bump version to {new_version}")
    fork("/usr/bin/git", "push")
    fork("./publish.sh")


if __name__ == "__main__":
    main()
