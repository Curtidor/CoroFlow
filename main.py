import asyncio
import functools
import random

from async_parallelizer import AsyncParallelizer


async def fetch_data(url: str) -> str:
    # Simulate fetching data from a URL
    sleep_time = random.randint(1, 3)
    await asyncio.sleep(sleep_time)  # Simulate varying fetch times
    return f"Data fetched from {url}"


async def main():
    # List of URLs to fetch data from
    urls = [
        "https://example.com/data1",
        "https://example.com/data2",
        "https://example.com/data3",
        "https://example.com/data4",
        "https://example.com/data5"
    ]

    # Create a list of asynchronous tasks
    tasks = [functools.partial(fetch_data, url) for url in urls]
    # Run the tasks concurrently with a maximum of 3 process groups
    async for result in AsyncParallelizer.threading_run_coros(tasks):
        print(result, "l")


asyncio.run(main())
