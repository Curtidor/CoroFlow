import unittest
import asyncio
from CroFlow.cro_flow import run_coros, threading_run_coros, _divide_coros  # noqa


class TestCoroFlow(unittest.IsolatedAsyncioTestCase):

    async def test_threading_run_coros(self):
        async def task_one():
            return 0

        coros = [task_one] * 5

        async for result in threading_run_coros(coros):
            self.assertEqual(result, 0)

    async def test_threading_run_coros_group_timeout_return_exceptions_true(self):
        timeout_errors = 0

        async def long_task():
            await asyncio.sleep(5)
            return "finished"

        coros = [long_task] * 3
        async for result in threading_run_coros(coros, group_timeout=2, return_exceptions=True):
            if isinstance(result, TimeoutError):
                timeout_errors += 1

        self.assertEqual(timeout_errors, 3)

    async def test_threading_run_coros_group_timeout_return_exceptions_false(self):
        async def long_task():
            await asyncio.sleep(5)
            return "finished"

        coros = [long_task] * 3
        results = [result async for result in threading_run_coros(coros, group_timeout=2, return_exceptions=False)]

        # No results should be yielded if all tasks time out
        self.assertEqual(len(results), 0)

    async def test_all_coroutines_timeout(self):
        timeout_errors = 0

        async def long_task():
            await asyncio.sleep(10)
            return "finished"

        coros = [long_task] * 3
        async for result in threading_run_coros(coros, timeout=1, return_exceptions=True):
            if isinstance(result, TimeoutError):
                timeout_errors += 1

        self.assertEqual(timeout_errors, 3)

    async def test_threading_run_coros_with_exceptions(self):
        error_responses = 0
        normal_responses = 0

        async def error_task():
            raise ValueError("Error in task")

        async def success_task():
            return "success"

        coros = [error_task, success_task]

        async for result in threading_run_coros(coros, return_exceptions=True):
            if isinstance(result, BaseException):
                error_responses += 1
            else:
                normal_responses += 1

        self.assertEqual(error_responses, 1)
        self.assertEqual(normal_responses, 1)

    async def test_threading_run_coros_exceptions_skipped(self):
        normal_responses = 0

        async def error_task():
            raise ValueError("Error in task")

        async def success_task():
            return "success"

        coros = [error_task, success_task]

        async for result in threading_run_coros(coros, return_exceptions=False):
            normal_responses += 1

        self.assertEqual(normal_responses, 1)

    async def test_empty_coros_threading_run_coros(self):
        coros = []
        results = [result async for result in threading_run_coros(coros)]
        self.assertEqual(len(results), 0)

    async def test_threading_run_coros_varied_execution_times(self):
        async def short_task():
            await asyncio.sleep(0.5)
            return "short"

        async def long_task():
            await asyncio.sleep(1)
            return "long"

        coros = [short_task, long_task]
        results = [result async for result in threading_run_coros(coros)]
        self.assertIn("short", results)
        self.assertIn("long", results)

    async def test_divide_coros(self):
        async def task_one():
            pass

        coros = [task_one] * 16
        grouped_coros = _divide_coros(coros, 4)
        for sub_group in grouped_coros:
            self.assertEqual(len(sub_group), 4)

        coros.clear()
        grouped_coros.clear()

        coros = [task_one] * 13
        grouped_coros = _divide_coros(coros, 4)

        for index in range(len(grouped_coros) - 1):
            self.assertEqual(len(grouped_coros[index]), 4)

        self.assertEqual(len(grouped_coros[-1]), 1)

    async def test_cancelled_coros(self):
        async def task_one():
            await asyncio.sleep(5)

        coros = [task_one] * 5
        task = asyncio.create_task(threading_run_coros(coros).__anext__())

        await asyncio.sleep(1)
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_group_timeout_with_mixed_return_exceptions(self):
        async def quick_task():
            return "quick"

        async def slow_task():
            await asyncio.sleep(5)

        coros = [quick_task, slow_task]
        results = []
        timeout_errors = 0

        async for result in threading_run_coros(coros, group_timeout=2, return_exceptions=True):
            if isinstance(result, TimeoutError):
                timeout_errors += 1
            else:
                results.append(result)

        self.assertIn("quick", results)
        self.assertEqual(timeout_errors, 1)


if __name__ == '__main__':
    unittest.main()
