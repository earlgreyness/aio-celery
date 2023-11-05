from __future__ import annotations

from typing import Any


def first_not_null(*items: Any) -> Any:
    for item in items:
        if item is not None:
            return item
    return None
