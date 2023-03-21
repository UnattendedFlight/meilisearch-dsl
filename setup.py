# pylint: disable=missing-docstring, invalid-name, line-too-long, R1732
from setuptools import setup, find_packages

setup(
    name="meilisearchdsl",
    version="{{VERSION_PLACEHOLDER}}",
    description="MeiliSearch DSL is a Python package providing a Django-like Q object syntax for"
    + " querying MeiliSearch, an open-source search engine."
    + " It simplifies search query building and offers a convenient wrapper"
    + " for MeiliSearch indexes and clients, streamlining search interactions"
    + " and improving maintainability.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    author="Adrian Leo",
    url="https://github.com/UnattendedFlight/meilisearch-dsl",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=[
        # List your package's dependencies here
        "meilisearch>=0.25.0",
    ],
)
