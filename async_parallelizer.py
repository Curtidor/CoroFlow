import asyncio
import logging
import threading
import traceback
import math

from asyncio import Queue, AbstractEventLoop
from concurrent.futures import ThreadPoolExecutor
from typing import Coroutine, List, Callable, AsyncGenerator, Any, Union


class _ErrorPlaceHolder:
    pass


class AsyncParallelizer:
    _logger = logging.getLogger(__name__)

    @classmethod
    async def threading_run_coros(cls, coros: List[Callable[..., Coroutine]], *args, max_process_groups: int = 4,
                                  timeout: float = 0, return_exceptions: bool = True,
                                  debug: bool = False, **kwargs) \
            -> AsyncGenerator[Union[Any, BaseException], None]:
        """
        Run a list of asynchronous coroutines concurrently using threads, dividing them into smaller groups.

        Args:
           -coros (List[Callable[..., Coroutine]]): List of asynchronous coroutines to run.
           -max_process_groups (int): Maximum number of process groups to create concurrently.
           -timeout (float): Maximum number of seconds to wait. 0 means wait indefinitely.
           -return_exceptions (bool): Whether exceptions/errors should be included in the yielded results.
           -debug (bool): if true prints traceback when an exceptions occur

        Returns:
           AsyncGenerator[Union[Any, BaseException]: Results or exceptions from the executed coroutines.

        Note: This method divides the provided coroutines into smaller groups to run concurrently, based on the
        'max_process_groups' parameter. For example, if 100 coroutines are provided with 'max_process_groups' set to
        4, the coroutines will be split into 4 groups of 25. Each group will be executed concurrently in its own
        thread. As results become available, they are yielded to the user, ensuring that the system remains
        responsive even if some coroutines are still being processed.

       """

        max_process_groups = max_process_groups if max_process_groups > 0 else 1

        coro_groups = cls._divide_coros(coros, max_process_groups)

        loop = await cls._get_loop()
        lock = threading.Lock()
        results_queue = Queue()

        def wrapper(sub_coros: List[Callable[..., Coroutine]]):
            async def async_wrapper():
                async for result in cls.run_coros(
                        coros=sub_coros, timeout=timeout,
                        return_exceptions=return_exceptions, debug=debug, loop=None, *args, **kwargs
                ):
                    with lock:
                        await results_queue.put(result)

            asyncio.run_coroutine_threadsafe(async_wrapper(), loop)

        with ThreadPoolExecutor(max_workers=max_process_groups) as executor:
            while coro_groups:
                executor.submit(wrapper, coro_groups.pop())

        for _ in coros:
            yield await results_queue.get()

    @classmethod
    async def run_coros(
            cls, coros: List[Callable[..., Coroutine]], *args,
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
               -timeout (float, optional): Maximum time in seconds to allow each coroutine to run. Defaults to
                    0 (no timeout).
               -return_exceptions (bool, optional): Whether to yield exceptions if they occur in coroutines.
               Defaults to True
               -debug (bool, optional): If True, exceptions will be printed to the console. Defaults to False.
               -loop (AbstractEventLoop, optional): An existing event loop to use. If None or closed, a new loop will
                    be created. Defaults to None.
               -**kwargs: Additional keyword arguments to pass to each coroutine.

           Yields:
               Union[Any, BaseException]: The result of each coroutine or an exception if `return_exceptions` is True.
           """

        if not loop or loop.is_closed() or not loop.is_running():
            # if the provided loop is None, closed, or not currently running,
            # we need to obtain a valid loop to execute the coroutines.
            # This ensures that the coroutines do not remain pending due to
            # an invalid or inactive loop.
            loop = await cls._get_loop()

        if not timeout:
            timeout = None

        results_queue = Queue()

        _ERROR_PLACEHOLDER_INSTANCE = _ErrorPlaceHolder()

        async def task_wrapper(coro: Callable[..., Coroutine]):
            async def execute_coro():
                return await coro(*args, **kwargs)

            try:
                coro_result = await asyncio.wait_for(execute_coro(), timeout=timeout)
            except BaseException as e:
                if debug:
                    cls._log_error(e)
                    traceback.print_exc()
                coro_result = e if return_exceptions else _ERROR_PLACEHOLDER_INSTANCE

            results_queue.put_nowait(coro_result)

        background_tasks = set()
        for c in coros:
            task = loop.create_task(task_wrapper(c))

            # add the task to the set, to avoid the task being garbage collected
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)

        for _ in coros:
            result = await results_queue.get()
            if result is _ERROR_PLACEHOLDER_INSTANCE:
                continue

            yield result

    @staticmethod
    async def _get_loop() -> asyncio.AbstractEventLoop:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.get_event_loop_policy().get_event_loop()

    @classmethod
    def _log_error(cls, e: BaseException):
        cls._logger.error("Exception occurred: %s", e)
        cls._logger.error("Exception type: %s", type(e).__name__)
        cls._logger.error("Exception args: %s", e.args)

    @staticmethod
    def _divide_coros(coros: List[Callable[..., Coroutine]], n: int) -> List[List[Callable[..., Coroutine]]]:
        """
        Divide a list of coroutines into smaller groups.

        Args:
            coros (List[Callable[..., Coroutine]]): List of coroutines.
            n (int): Number of subgroups to create.

        Returns:
            List[List[Callable[..., Coroutine]]]: Subgroups of coroutines.
        """
        coro_groups = []

        n = math.ceil(len(coros) / n)
        for i in range(0, len(coros), n):
            sub_task_list = coros[i:i + n]

            coro_groups.append(sub_task_list)

        return coro_groups
