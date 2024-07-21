import unittest
import asyncio
from unittest.mock import patch

from async_parallelizer import AsyncParallelizer


class TestAsyncParallelizer(unittest.IsolatedAsyncioTestCase):

    async def test_threading_run_coros(self):
        async def task_one():
            return 0

        coros = [task_one] * 5

        async for result in AsyncParallelizer.threading_run_coros(coros):
            self.assertEqual(result, 0)

    async def test_run_coros(self):
        async def task_one():
            return 0

        coros = [task_one] * 5

        async for result in AsyncParallelizer.run_coros(coros):
            self.assertEqual(result, 0)

    async def test_return_exceptions_true(self):
        error_responses = 0
        normal_responses = 0

        async def task_one():
            raise ValueError()

        async def task_two():
            return "passed"

        coros = [task_one, task_two]

        async for result in AsyncParallelizer.run_coros(coros, return_exceptions=True):
            if isinstance(result, BaseException):
                error_responses += 1
            else:
                normal_responses += 1

        self.assertEqual(error_responses, 1)
        self.assertEqual(normal_responses, 1)

    async def test_return_exceptions_false(self):
        error_responses = 0
        normal_responses = 0

        async def task_one():
            raise ValueError()

        async def task_two():
            return "passed"

        coros = [task_one, task_two]

        async for result in AsyncParallelizer.run_coros(coros, return_exceptions=False):
            if isinstance(result, BaseException):
                error_responses += 1
            else:
                normal_responses += 1

        self.assertEqual(error_responses, 0)
        self.assertEqual(normal_responses, 1)

    async def test_timeout(self):
        timeout_error = 0
        normal_responses = 0

        async def task_one():
            await asyncio.sleep(10)
            return "passed"

        async def task_two():
            return "passed"

        coros = [task_one, task_two]

        async for result in AsyncParallelizer.run_coros(coros, timeout=1):
            if isinstance(result, asyncio.TimeoutError):
                timeout_error += 1
            else:
                normal_responses += 1

        self.assertEqual(timeout_error, 1)
        self.assertEqual(normal_responses, 1)

    async def test_empty_coros(self):
        coros = []

        results = [result async for result in AsyncParallelizer.run_coros(coros)]
        self.assertEqual(len(results), 0)

    async def test_varied_execution_times(self):
        async def task_one():
            await asyncio.sleep(1)
            return "task_one"

        async def task_two():
            await asyncio.sleep(0.5)
            return "task_two"

        coros = [task_one, task_two]

        results = [result async for result in AsyncParallelizer.run_coros(coros)]
        self.assertIn("task_one", results)
        self.assertIn("task_two", results)

    async def test_mocked_run_coros(self):
        async def task_one():
            return "task_one"

        async def mocked_run_coros(c):
            for coro in c:
                yield await coro()

        coros = [task_one]

        with patch.object(AsyncParallelizer, 'run_coros', return_value=mocked_run_coros(coros)) as mocked_method:
            results = [result async for result in AsyncParallelizer.run_coros(coros)]

            self.assertEqual(results, ["task_one"])

            mocked_method.assert_called_once_with(coros)


if __name__ == '__main__':
    unittest.main()
