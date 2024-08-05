# CoroFlow

CoroFlow is a Python library for running asynchronous coroutines concurrently, yielding results as soon as they are available. It supports both single-threaded and multi-threaded execution, offering flexibility for efficiently managing complex asynchronous tasks.

 ## Why Use CoroFlow?
* **Instant Results:** CoroFlow delivers results as soon as they become available, allowing your application to process and respond to data without waiting for all tasks to complete. This can significantly improve the responsiveness and user experience of applications that rely on real-time data processing.

* **Ideal for High-Performance Applications:** Perfect for scenarios where rapid execution and result retrieval are crucial, such as in real-time data processing, streaming applications, or services requiring quick response times.

* **Versatile Use Cases:** Whether you're building web servers to web scrappers, data pipelines, or other concurrent systems, CoroFlow helps manage complex asynchronous tasks efficiently. Its flexibility makes it suitable for both small-scale projects and large, distributed systems where timely results are essential.

## Features:
* **Concurrent Execution:** Run multiple coroutines concurrently in the same thread or across various threads.

* **Timeout Handling:** Specify a timeout for coroutine execution to prevent tasks from running indefinitely.

* **Exception Handling:** Choose whether to return exceptions as results or handle them silently.

* **Debugging Support:** Option to print stack traces for exceptions that occur during coroutine execution.

## Basic Usage

Here's a simple example of how to use CoroFlow to run multiple coroutines concurrently:

```py
import asyncio
from CoroFlow import run_coros


async def task_one():
    await asyncio.sleep(3)
    return "Task One Completed"


async def task_two():
    await asyncio.sleep(1)
    return "Task Two Completed"


async def main():
    coros = [task_one, task_two]

    async for result in run_coros(coros):
        print(result)


asyncio.run(main())
```

## Multi-threaded Execution
To run coroutines across multiple threads, use threading_run_coros:

```py
import asyncio
from CoroFlow import threading_run_coros


async def task_one():
    await asyncio.sleep(2.9)
    return "Task One Completed"


async def task_two():
    await asyncio.sleep(1.3)
    return "Task Two Completed"


async def main():
    coros = [task_one, task_two]

    # max_process_groups is the number of threads to use
    async for result in threading_run_coros(coros, max_process_groups=2):
        print(result)


asyncio.run(main())
```

## Handling Exceptions
you can configure CoroFlow to return exceptions as part of the results (exceptions are returned by default):

```py
import asyncio
from CoroFlow import run_coros


async def task_one():
    raise ValueError("An error occurred in Task One")


async def task_two():
    return "Task Two Completed"


async def main():
    coros = [task_one, task_two]

    async for result in run_coros(coros, return_exceptions=True):
        print(result, type(result))

        asyncio.run(main())
```
### Output
```
>>> An error occurred in Task One <class 'ValueError'>
>>> Task Two Completed
```

## Setting Timeouts

Specify a timeout to limit the execution time of each coroutine:

```py
import asyncio
from CoroFlow import run_coros


async def task_one():
    await asyncio.sleep(5)
    return "Task One Completed"


async def task_two():
    return "Task Two Completed"


async def main():
    coros = [task_one, task_two]

    async for result in run_coros(coros, timeout=2):
        print(result, type(result))

        asyncio.run(main())

```

### Output
```
>>> Task Two Completed <class 'str'>
>>> <class 'TimeoutError'>
```

# API Reference

**`run_coros(coros, *args, timeout=0, return_exceptions=True, debug=False, loop=None, **kwargs)`**

Runs a list of coroutines concurrently.

* **coros:** List of coroutines to run.
* **timeout:** Maximum time in seconds to allow each coroutine to run. Defaults to 0 (no timeout).
* **return_exceptions:** Whether to return exceptions as part of the results. Defaults to True.
* **debug:** If True, prints stack traces for exceptions. Defaults to False.
* **loop:** Event loop to use. If None or closed, a new loop is created.

**`threading_run_coros(coros, *args, max_process_groups=4, timeout=0, return_exceptions=True, debug=False, **kwargs)`**

Runs a list of coroutines concurrently using multiple threads.

* **coros:** List of coroutines to run.
* **max_process_groups:** Maximum number of process groups to create concurrently. Defaults to 4.
* **timeout:** Maximum time in seconds to allow each coroutine to run. Defaults to 0 (no timeout).
* **return_exceptions:** Whether to return exceptions as part of the results. Defaults to True.
* **debug:** If True, prints stack traces for exceptions. Defaults to False.

# Comparison: `asyncio.as_completed` vs. `CoroFlow`

When working with asynchronous coroutines in Python, you have different tools to handle concurrent execution. Below is a comparison between `asyncio.as_completed` and the custom `coro_flow` function to help you decide which is best suited for your needs.

### `asyncio.as_completed`

**Purpose:**
- Provides an iterator that yields coroutines as they complete, in the order they finish.

**Key Features:**
- **Concurrency:** Runs coroutines concurrently within the asyncio event loop.
- **Simplicity:** Straightforward usage for processing coroutines as they complete.
- **Error Handling:** Does not handle exceptions or provide debugging capabilities.
- **Timeout Handling:** Does not support per-coroutine timeouts.
- **Event Loop:** Operates within the existing asyncio event loop.

**Example Usage:**
```python
coros = [coro1(), coro2(), coro3()]
async for task in asyncio.as_completed(coros):
    result = await task
    print(result)
```

**Best For:**

* Simple use cases where you need to handle coroutines as they finish without additional error handling or timeout features.

### `CoroFlow`
**Purpose:**

* Yields the results from a list of coroutines concurrently using threading (`optionally`) and asyncio, with features for error handling, timeout control, and event loop management.

**Key Features:**

* **Concurrency:** Offers flexibility by allowing coroutines to be executed either within the asyncio event loop or using threading (via `ThreadPoolExecutor`). When     using threading, coroutines are divided into groups, and each group is processed concurrently in different threads. This approach helps manage the load on the        main event loop and can efficiently handle large numbers of coroutines by distributing the workload across multiple threads and event loops.
* **Error Handling:** Provides options for handling and returning exceptions, with optional debugging and error logging.
* **Timeout Handling:** Allows specifying a timeout for each coroutine.
* **Event Loop Management:** Can use an existing event loop or create a new one if necessary.
* **Grouping:** `(If using the threading method)` Divides coroutines into smaller groups for concurrent execution.
  
Example Usage:

```py
async for result in run_coros([coro1, coro2, coro3], timeout=5, debug=True):
    print(result)
```
**Best For:**

* Scenarios requiring detailed control over coroutine execution, including error handling, timeout management, and concurrent execution using threads.

## Summary

| Feature             | `asyncio.as_completed`                            | `CoroFlow`                               |
|---------------------|---------------------------------------------------|--------------------------------------------------|
| **Purpose**         | Yields coroutines as they complete                | Yields the results of coroutines as they become available     |
| **Concurrency**     | Uses asyncio event loop only                      | Uses asyncio event loop, with the additional choice of threading         |
| **Error Handling**  | No built-in error handling                        | Handles exceptions with optional logging        |
| **Timeout Handling**| No Supports per-coroutine timeouts                | Supports per-coroutine timeouts                 |
| **Event Loop**      | Uses existing event loop                          | Can create or use an existing event loop         |
| **Simplicity**      | Simple and straightforward                        | More complex but offers extensive control      |


## Contributing
Contributions are welcome! Please open an issue or submit a pull request with your changes.

## License
This project is licensed under the MIT License.
