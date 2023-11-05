from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    import redis.asyncio


def create_redis_connection_pool(
    *,
    url: str,
    pool_size: int,
) -> redis.asyncio.BlockingConnectionPool:
    import redis.backoff
    from redis.asyncio import BlockingConnectionPool
    from redis.asyncio.retry import Retry

    default_base = getattr(redis.backoff, "DEFAULT_BASE", 0.008)
    default_cap = getattr(redis.backoff, "DEFAULT_CAP", 0.512)

    return cast(
        BlockingConnectionPool,
        BlockingConnectionPool.from_url(
            url=url,
            max_connections=pool_size,
            timeout=None,
            socket_timeout=5,
            retry_on_timeout=True,
            retry=Retry(
                backoff=redis.backoff.EqualJitterBackoff(
                    cap=default_cap,
                    base=default_base,
                ),
                retries=5,
            ),
        ),
    )
