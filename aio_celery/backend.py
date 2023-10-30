from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    import redis.asyncio


def create_redis_connection_pool(
    *,
    url: str,
    pool_size: int,
) -> "redis.asyncio.BlockingConnectionPool":
    from redis.asyncio import BlockingConnectionPool
    from redis.asyncio.retry import Retry
    from redis.backoff import default_backoff

    assert url.startswith("redis://")

    return cast(
        BlockingConnectionPool,
        BlockingConnectionPool.from_url(
            url=url,
            max_connections=pool_size,
            timeout=None,
            socket_timeout=5,
            retry_on_timeout=True,
            retry=Retry(
                backoff=default_backoff(),
                retries=5,
            ),
        ),
    )
