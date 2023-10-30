from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import redis.asyncio


def create_redis_connection_pool(
    *,
    url: str,
    pool_size: int,
) -> "redis.asyncio.BlockingConnectionPool":
    from redis.asyncio import BlockingConnectionPool
    from redis.asyncio.retry import Retry
    from redis.backoff import EqualJitterBackoff

    try:
        from redis.backoff import DEFAULT_BASE, DEFAULT_CAP
    except ImportError:
        # Maximum backoff between each retry in seconds
        DEFAULT_CAP = 0.512
        # Minimum backoff between each retry in seconds
        DEFAULT_BASE = 0.008

    assert url.startswith("redis://")

    return BlockingConnectionPool.from_url(
        url=url,
        max_connections=pool_size,
        timeout=None,
        socket_timeout=5,
        retry_on_timeout=True,
        retry=Retry(
            backoff=EqualJitterBackoff(cap=DEFAULT_CAP, base=DEFAULT_BASE),
            retries=5,
        ),
    )
