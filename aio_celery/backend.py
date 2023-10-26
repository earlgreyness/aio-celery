from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    import redis.asyncio


def create_redis_pool(
    url: str,
    pool_size: int,
) -> "redis.asyncio.BlockingConnectionPool":
    import redis.asyncio
    import redis.asyncio.retry
    import redis.backoff

    return cast(
        redis.asyncio.BlockingConnectionPool,
        redis.asyncio.BlockingConnectionPool.from_url(
            url=url,
            max_connections=pool_size,
            timeout=None,
            socket_timeout=5,
            retry_on_timeout=True,
            retry=redis.asyncio.retry.Retry(
                backoff=redis.backoff.default_backoff(),
                retries=5,
            ),
        ),
    )
