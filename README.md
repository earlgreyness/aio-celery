AsyncIO Celery Worker
=====================

Overview
--------

This project is an alternative asyncio implementation of the Celery Worker protocol.

* Feature parity with upstream Celery project IS NOT the goal
* Support of different brokers/result backends IS NOT the goal

The following are the actual goals:
* The code base of this project must be as simple as possible to understand and reason about
* There should be as little code as possible (less code means less bugs)
* There should be as little external dependencies as possible
* This project must not have celery as an external dependency 

Usage
-----

```python
import asyncio
from aio_celery import Celery

app = Celery()

@app.task(name='add-two-numbers')
async def mul(a, b):
    await asyncio.sleep(5)
    return a + b
```

Then run worker:

```bash
$ aio_celery -A hello:app worker --concurrency=50000
```

Queue some tasks:

```python
import asyncio
from hello import add, app

async def fill_queue():
    async with app.setup():
        for n in range(50000):
            await add.delay(n, n)

asyncio.run(fill_queue())
```
