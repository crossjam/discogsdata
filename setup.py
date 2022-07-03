from setuptools import setup
import os

VERSION = "0.1"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="discogsdata",
    description="CLI for exploring/exploiting a DB populated from Discogs Data",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Brian M. Dennis",
    url="https://github.com/crossjam/discogsdata",
    project_urls={
        "Issues": "https://github.com/crossjam/discogsdata/issues",
        "CI": "https://github.com/crossjam/discogsdata/actions",
        "Changelog": "https://github.com/crossjam/discogsdata/releases",
    },
    license="Apache License, Version 2.0",
    version=VERSION,
    packages=["discogsdata"],
    entry_points="""
        [console_scripts]
        discogsdata=discogsdata.cli:cli
    """,
    install_requires=["click", "psycopg2"],
    extras_require={"test": ["pytest", "psycopg2", "tabulate"]},
    tests_require=["discogsdata[test]", "psycopg2"],
    python_requires=">=3.6",
)
