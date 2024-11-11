import asyncio
import functools
import random

from CroFlow import run_coros, threading_run_coros


async def fetch_data(url: str) -> str:
    # Simulate fetching data from a URL
    sleep_time = random.randint(1, 5)
    if sleep_time == 5:
        raise Exception("TO MUCH DOWN TIME")

    await asyncio.sleep(sleep_time)  # Simulate varying fetch times
    return f"Data fetched from {url}"


async def main():

    # List of URLs to fetch data from
    urls = [
        "https://example.com/data1",
        "https://example.com/data2",
        "https://example.com/data3",
        "https://example.com/data4",
        "https://example.com/data5",
        "https://example.com/data6",
        "https://example.com/data7",
        "https://example.com/data8",
        "https://example.com/data9",
        "https://example.com/data10"
    ]

    # Create a list of asynchronous tasks
    tasks = [functools.partial(fetch_data, url) for url in urls]

    # Run the tasks concurrently with a maximum of 3 process groups
    async for result in run_coros(tasks, timeout=5, debug=True):
        print(result, type(result))


asyncio.run(main())
