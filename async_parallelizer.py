import asyncio
import functools
import math

from concurrent.futures import ThreadPoolExecutor
from typing import Coroutine, List, Callable, AsyncGenerator, Any


class TaskRef:
    def __init__(self):
        self.task: asyncio.Task = None  # noqa


class AsyncParallelizer:
    @classmethod
    async def run_coros(cls, coros: List[Callable[..., Coroutine]], *args, max_process_groups: int = 4, return_exceptions: bool = True, **kwargs) -> AsyncGenerator[Any | BaseException, None]:
        """
        Run a list of asynchronous coroutines concurrently using threads, dividing them into smaller groups.

        :param
            - coros (List[Callable[..., Coroutine]]): List of asynchronous coroutines to run.
            - max_process_groups (int): Maximum number of process groups to create concurrently.
            - return_exceptions: whether exceptions/errors should be included in the yielded results

        :return:
            AsyncGenerator[Any | BaseException]: Results or exceptions from the executed coroutines.

        Note:
            This method divides the provided coroutines into smaller groups to run concurrently, based on the 'max_process_groups' parameter.
            For example, if 100 coroutines are provided with 'max_process_groups' set to 4, the coroutines will be split into 4 groups of 25.
            Each group will be executed concurrently in its own thread. As results become available, they are yielded to the user,
            ensuring that the system remains responsive even if some coroutines are still being processed.

        """

        max_process_groups = max_process_groups if max_process_groups > 0 else 1

        coro_groups = cls._divide_coros(coros, max_process_groups)
        running_futures = []

        task_ref = TaskRef()

        results_queue = asyncio.Queue()
        executor = ThreadPoolExecutor(max_workers=max_process_groups)
        while coro_groups:
            sub_set_of_coros = coro_groups.pop()

            future = asyncio.wrap_future(
                executor.submit(cls._async_wrapper, sub_set_of_coros, results_queue, return_exceptions, *args, *kwargs)
            )
            running_futures.append(future)

            callback = functools.partial(cls._on_future_finish, running_futures, task_ref)
            future.add_done_callback(callback)

        async def get_queue_item() -> Any:
            return await results_queue.get()

        while running_futures:
            task_ref.task = asyncio.create_task(get_queue_item())
            try:
                await task_ref.task
            except asyncio.CancelledError:
                # if the task is cancelled all the running futures have finished, so we can break out of the loop
                break
            yield task_ref.task.result()

        # yield any remaining item in the queue
        while not results_queue.empty():
            yield results_queue.get_nowait()

        executor.shutdown(wait=True)

    @classmethod
    def _divide_coros(cls, coros: List[Callable[..., Coroutine]], n: int) -> List[List[Callable[..., Coroutine]]]:
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

    @classmethod
    def _async_wrapper(cls, coros: List[Callable[..., Coroutine]], results_queue: asyncio.Queue, return_exceptions: bool, *args, **kwargs) -> None:
        """
        Execute a list of asynchronous coroutines and put results into a queue.

        Args:
            coros (List[Callable[..., Coroutine]]): List of asynchronous coroutines.
            results_queue (asyncio.Queue): Queue to store results.
            return_exceptions: whether exceptions/errors should be included in the yielded results

        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        tasks = [loop.create_task(coro(*args, **kwargs)) for coro in coros]

        coro = cls._wait_for_task(tasks, return_exceptions)
        for result in loop.run_until_complete(coro):
            results_queue.put_nowait(result)

        loop.close()

    @staticmethod
    async def _wait_for_task(tasks: List[asyncio.Task], return_exceptions: bool) -> List[Any | BaseException]:
        """
        Wait for a list of asynchronous tasks to complete.

        Args:
            tasks (List[asyncio.Task]): List of asynchronous tasks.
            return_exceptions: whether exceptions/errors should be included in the yielded results

        Returns:
            List[Any | BaseException]: Results or exceptions from the completed tasks.
        """
        r = []

        results = await asyncio.gather(*tasks, return_exceptions=return_exceptions)
        for result in results:
            r.append(result)

        return r

    @staticmethod
    def _on_future_finish(running_futures: List[asyncio.Future], task_ref: TaskRef, fut: asyncio.Future) -> None:
        """
        Callback function to handle completion of a future.

        Args:
            running_futures (List[asyncio.Future]): List of running futures.
            task_ref (TaskRef): Reference to the current running task.
            fut (asyncio.Future): Completed future.
        """
        running_futures.remove(fut)

        if len(running_futures) == 0 and task_ref.task:
            task_ref.task.cancel()
