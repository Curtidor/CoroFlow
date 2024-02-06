import asyncio
from async_parallelizer import AsyncParallelizer

from typing import Callable


async def fetch_data(url_generator: Callable) -> str:
    # Simulate fetching data from a URL
    await asyncio.sleep(1)  # Simulate varying fetch times
    return f"Data fetched from {url_generator()}"


async def main():
    # List of URLs to fetch data from
    urls = [
        "https://example.com/data1",
        "https://example.com/data2",
        "https://example.com/data3",
        "https://example.com/data4",
        "https://example.com/data5"
    ]

    def url_gen() -> str:
        return urls.pop()

    # Create a list of asynchronous tasks
    tasks = [fetch_data for url in urls]

    # Run the tasks concurrently with a maximum of 3 process groups
    async for item in AsyncParallelizer.run_coros(tasks, url_gen, max_process_groups=3):
        print(item)


asyncio.run(main())
