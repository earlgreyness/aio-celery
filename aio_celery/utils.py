from __future__ import annotations

import uuid
from typing import Any


def first_not_null(*items: Any) -> Any:
    for item in items:
        if item is not None:
            return item
    return None


def generate_consumer_tag(*, prefix: str = "", channel_number: int = 1) -> str:
    return f"{prefix}ctag{channel_number}.{uuid.uuid4().hex}"
