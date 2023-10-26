aio-celery
==========

What is aio-celery?
-------------------

This project is an alternative independent asyncio implementation of [Celery](https://docs.celeryq.dev).


Quoting Celery [documentation](https://docs.celeryq.dev/en/latest/getting-started/introduction.html#what-s-a-task-queue):

> Celery is written in Python, but the protocol can be implemented in any language.

And aio-celery does exactly this, it (re)implements Celery protocol (in Python)
in order to unlock access to asyncio tasks and workers. 

The most notable feature of aio-celery is that it does not depend on Celery codebase.
It is written completely from scratch as a thin wrapper around [aio-pika](https://github.com/mosquito/aio-pika)
(which is an asyncronous RabbitMQ python driver)
and it has no other dependencies (except for [redis-py](https://github.com/redis/redis-py) for result backend support, but this dependency is optional).

There have been attempts to create asyncio Celery Pools before, and [celery-pool-asyncio](https://pypi.org/project/celery-pool-asyncio/)
is one such example, but its implementation, due to convoluted structure 
of the original Celery codebase, is (by necessity) full of monkeypatching and other
fragile techniques. This fragility was apparently the [reason](https://github.com/kai3341/celery-pool-asyncio/issues/29)
why this library became incompatible with Celery version 5.

Celery project itself clearly [struggles](https://github.com/celery/celery/issues/7874) with implementing Asyncio Coroutine support,
constantly delaying this feature due to apparent architectural difficulties.

This project was created in an attempt to solve the same problem but using the opposite approach.
It implements only a limited (but still usable — that is the whole point) subset of Celery functionality
without relying on Celery code at all — the goal is to mimic the basic
wire protocol and to support a subset of Celery API minimally required for running and manipulating
tasks.

Features
--------

What is supported:

* Basic tasks API: `@app.task` decorator, `delay` and `apply_async` task methods, `AsyncResult` class etc.
* Everything is asyncio-friendly and awaitable
* Asyncronous Celery worker that is started from the command line
* Routing and publishing options such as `countdown`, `eta`, `queue`, `priority`, etc.
* Task retries
* Only RabbitMQ as a message broker
* Only Redis as a result backend

Important design decisions for aio-celery:

* Complete feature parity with upstream Celery project is not the goal
* The parts that are implemented mimic original Celery API as close as possible, down to
class and attribute names
* The codebase of this project is kept as simple and as concise, it strives to be easy to understand and reason about
* The codebase is maintained to be as small as possible – the less code, the fewer bugs
* External dependencies are kept to a minimum for the same purpose
* This project must not at any point have celery as its external dependency 

Installation
------------
Install and update using [pip](https://pip.pypa.io/en/stable/getting-started/):

```bash
pip install -U aio-celery
```

If you intend to use Redis result backend for storing task results, run this command:
```bash
pip install -U aio-celery[redis]
```

Usage
-----
Define `Celery` application instance and register a task:
```python
# hello.py
import asyncio
from aio_celery import Celery

app = Celery()

@app.task
async def add(a, b):
    await asyncio.sleep(5)
    return a + b
```

Then run worker:

```bash
$ aio_celery worker hello:app
```

Queue some tasks:

```python
# publish.py
import asyncio
from hello import add, app

async def publish():
    async with app.setup():
        tasks = [add.delay(n, n) for n in range(50000)]
        await asyncio.gather(*tasks)

asyncio.run(publish())
```
```bash
$ python3 publish.py
```
The last script concurrently publishes 50000 messages to RabbitMQ. It takes about 8 seconds to finish,
with gives average publishing rate of about 6000 messages per second.


Advanced Example
----------------

```python
from aio_celery import Celery

app = Celery()
app.conf.update(
    result_backend="redis://localhost:6379/1",
)

@app.task(name='do-something')
async def do():
    result = await foo.apply_async(args=[1,2,3], countdown=5)
    value = await result.get(timeout=10)
```

Adding context
--------------


```python
import contextlib
import asyncpg
from aio_celery import Celery

app = Celery()

@app.define_app_context
@contextlib.asynccontextmanager
async def setup_context():
    async with asyncpg.create_pool("postgresql://localhost:5432", max_size=10) as pool:
        yield {"pool": pool}

@app.task
async def get_postgres_version():
    async with app.context["pool"].acquire() as conn:
        version = await conn.fetchval("SELECT version()")
    return version

```