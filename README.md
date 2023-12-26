# discogsdata

<!---
[![PyPI](https://img.shields.io/pypi/v/discogsdata.svg)](https://pypi.org/project/discogsdata/)
[![Changelog](https://img.shields.io/github/v/release/crossjam/discogsdata?include_prereleases&label=changelog)](https://github.com/crossjam/discogsdata/releases)
--->
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/crossjam/discogsdata/blob/master/LICENSE)
[![Tests](https://github.com/crossjam/discogsdata/workflows/Test/badge.svg)](https://github.com/crossjam/discogsdata/actions?query=workflow%3ATest)

CLI for exploring/exploiting a DB populated from Discogs Data

## Installation

Install this tool using `pip`:

    $ pip install discogsdata

## Usage

Usage instructions go here.

First you’ll need a database of Discogs Data. This `discogsdata` CLI
tool relies on data imported using the
[discogs-xml2db](https://github.com/philipmat/discogs-xml2db). It also
currently works strictly for the [PostgreSQL](https://postgresql.org)
database.


## Development

To contribute to this tool, first checkout the code. Then create a new virtual environment:

	git clone https://github.com/crossjam/discogsdata
    cd discogsdata
    python -mvenv venv
    source venv/bin/activate

Or if you are using `pipenv`:

    pipenv shell

Now install the dependencies and test dependencies:

    pip install -e '.[test]'

To run the tests:

    pytest
