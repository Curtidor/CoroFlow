from setuptools import setup, find_packages
import os

here = os.path.abspath(os.path.dirname(__file__))

VERSION = '0.0.0'
DESCRIPTION = (
    "CoroFlow is a Python library for running asynchronous coroutines concurrently, yielding results as soon as they "
    "are available. It supports both single-threaded and multi-threaded execution, offering flexibility for efficiently"
    "managing complex asynchronous tasks. handling exceptions and timeouts to optimize performance "
    "and manage complex workflows."
)

# Setting up
setup(
    name="CoroFlow",
    version=VERSION,
    author="Tanner Matos",
    author_email="tannermatos18@gmail.com",
    description=DESCRIPTION,
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=['pytest'],
    keywords=['CoroFlow', 'asynchronous', 'threading', 'parallel execution', 'coroutines',
              'task management', 'performance optimization', 'Python 3'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: Microsoft :: Windows",
    ]
)
