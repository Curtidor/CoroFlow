import asyncio
import logging
import threading
import traceback
import math
import json

from asyncio import Queue, AbstractEventLoop
from concurrent.futures import ThreadPoolExecutor
from typing import Coroutine, List, Callable, AsyncGenerator, Any, Union

_logger = logging.getLogger(__name__)


class _ErrorPlaceHolder:
    """
    A placeholder object used to indicate that an exception occurred in a coroutine.

    This sentinel value is used internally in `run_coros` to distinguish between
    actual results and exceptions when `return_exceptions` is set to False.
    """
    pass


async def threading_run_coros(
        coros: List[Callable[..., Coroutine]], *args, max_process_groups: int = 4,
        timeout: float = 0, group_timeout: float = 0,
        return_exceptions: bool = True, debug: bool = False, **kwargs
) -> AsyncGenerator[Union[Any, BaseException], None]:
    """
    Run a list of asynchronous coroutines concurrently using threads, dividing them into smaller groups.

    Args:
       - coros (List[Callable[..., Coroutine]]): List of asynchronous coroutines to run.
       - max_process_groups (int): Maximum number of process groups to create concurrently.
       - timeout (float): Maximum time to allow each coroutine to run. 0 means no timeout.
       - group_timeout (float): Maximum time to allow each group of coroutines to run. 0 means no timeout.
       - return_exceptions (bool): Whether exceptions/errors should be included in the yielded results.
       - debug (bool): If true, prints traceback when an exception occurs.

    Returns:
       AsyncGenerator[Union[Any, BaseException], None]: Results or exceptions from the executed coroutines.
    """
    max_process_groups = max(1, max_process_groups)
    coro_groups = _divide_coros(coros, max_process_groups)
    loop = await _get_loop()
    lock = threading.Lock()
    results_queue = Queue()

    _ERROR_PLACEHOLDER_INSTANCE = _ErrorPlaceHolder()

    async def process_coros(sub_coros: List[Callable[..., Coroutine]]):
        async for result in run_coros(
                coros=sub_coros, timeout=timeout,
                return_exceptions=True, debug=debug, loop=None, *args, **kwargs
        ):
            if not return_exceptions and isinstance(result, BaseException):
                result = _ERROR_PLACEHOLDER_INSTANCE

            with lock:
                await results_queue.put(result)

    async def group_wrapper(sub_coros: List[Callable[..., Coroutine]]):
        try:
            if group_timeout > 0:
                async with asyncio.timeout(group_timeout):
                    await process_coros(sub_coros)
            else:
                await process_coros(sub_coros)
        except asyncio.TimeoutError:
            error_value = TimeoutError(f"Group execution exceeded the timeout limit. {sub_coros}") \
                if return_exceptions else _ERROR_PLACEHOLDER_INSTANCE
            # add TimeoutError for each coroutine in the group
            for _ in sub_coros:
                await results_queue.put(error_value)

    def thread_wrapper(sub_coros: List[Callable[..., Coroutine]]):
        asyncio.run_coroutine_threadsafe(group_wrapper(sub_coros), loop)

    with ThreadPoolExecutor(max_workers=max_process_groups) as executor:
        while coro_groups:
            executor.submit(thread_wrapper, coro_groups.pop())

    for _ in coros:
        result = await results_queue.get()
        if result is _ERROR_PLACEHOLDER_INSTANCE:
            continue

        yield result


async def run_coros(
        coros: List[Callable[..., Coroutine]], *args,
        timeout: float = 0, return_exceptions: bool = True,
        debug: bool = False, loop: AbstractEventLoop = None, **kwargs
) -> AsyncGenerator[Union[Any, BaseException], None]:
    """
    Runs a list of coroutines concurrently with optional timeout and error handling.

    This method takes a list of coroutine functions, executes them concurrently, and yields their results as
    they complete. If a timeout is specified, each coroutine will be allowed to run for up to the specified
    number of seconds.

    Args:
        -coros (List[Callable[..., Coroutine]]): A list of coroutine functions to be executed.
        -*args: Positional arguments to pass to each coroutine.
        -timeout (float, optional): Maximum time in seconds to allow each coroutine to run. Defaults to 0 (no timeout).
        -return_exceptions (bool, optional): Whether to yield exceptions if they occur in coroutines. Defaults to True.
        -debug (bool, optional): If True, exceptions will be printed to the console. Defaults to False.
        -loop (AbstractEventLoop, optional): An existing event loop to use. If None or closed, a new loop will be
            created. Defaults to None.
        -**kwargs: Additional keyword arguments to pass to each coroutine.

    Yields:
        Union[Any, BaseException]: The result of each coroutine or an exception if `return_exceptions` is True.
    """

    if not loop or loop.is_closed() or not loop.is_running():
        # if the provided loop is None, closed, or not currently running,
        # we need to obtain a valid loop to execute the coroutines.
        # This ensures that the coroutines do not remain pending due to
        # an invalid or inactive loop.
        loop = await _get_loop()

    if not timeout:
        timeout = None

    results_queue = Queue()
    _ERROR_PLACEHOLDER_INSTANCE = _ErrorPlaceHolder()

    async def task_wrapper(coro: Callable[..., Coroutine]):
        async def execute_coro():
            return await coro(*args, **kwargs)

        try:
            coro_result = await asyncio.wait_for(execute_coro(), timeout=timeout)
        except asyncio.TimeoutError:
            coro_result = TimeoutError("Coro execution exceeded the timeout limit") if return_exceptions \
                else _ERROR_PLACEHOLDER_INSTANCE
        except asyncio.CancelledError:
            coro_result = asyncio.CancelledError("Coro canceled") if return_exceptions \
                else _ERROR_PLACEHOLDER_INSTANCE
        except BaseException as e:
            if debug:
                _log_error(_logger, e)
            coro_result = e if return_exceptions else _ERROR_PLACEHOLDER_INSTANCE

        results_queue.put_nowait(coro_result)

    background_tasks = set()
    try:
        # launches the coros
        for c in coros:
            task = loop.create_task(task_wrapper(c))
            # add the task to a collection to prevent them from being garbage collected
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)

        # waits for the results of the launched coros
        for _ in coros:
            result = await results_queue.get()
            if result is _ERROR_PLACEHOLDER_INSTANCE:
                continue

            yield result

    finally:
        # cancel any remaining tasks if the coroutine is cancelled
        for task in background_tasks:
            if not task.done():
                task.cancel()


async def _get_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        # no running loop; create and set a new event loop for the current thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def _log_error(logger: logging.Logger, e: BaseException):
    exc_type = type(e).__name__
    thread_name = threading.current_thread().name
    stack_trace = traceback.format_exc()

    log_message = (
        f"[ERROR] {exc_type}: {str(e)}\n"
        f"Thread: {thread_name}\n"
        f"Stack Trace:\n{stack_trace.strip()}"
    )
    logger.error(log_message)


def _divide_coros(coros: List[Callable[..., Coroutine]], n: int) -> List[List[Callable[..., Coroutine]]]:
    """
    Divide a list of coroutines into smaller groups.

    Args:
        coros (List[Callable[..., Coroutine]]): List of coroutines.
        n (int): Number of subgroups to create.

    Returns:
        List[List[Callable[..., Coroutine]]]: Subgroups of coroutines.
    """
    if not coros:
        return []  # if there are no coroutines, return an empty list

    if n <= 0:
        n = 1  # default to 1 group if n is 0 or negative

    n = min(n, len(coros))  # ensure n does not exceed the number of coroutines
    group_size = math.ceil(len(coros) / n)

    return [coros[i:i + group_size] for i in range(0, len(coros), group_size)]
