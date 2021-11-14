import logging

import click
import psycopg2

from .logconfig import DEFAULT_LOG_FORMAT, logging_config


@click.group()
@click.version_option()
@click.option(
    "--log-format",
    type=click.STRING,
    default=DEFAULT_LOG_FORMAT,
    envvar="DISCOGSDATA_LOG_FORMAT",
    help="Python logging format string",
)
@click.option(
    "--log-level",
    default="ERROR",
    help="Python logging level",
    show_default=True,
    envvar="DISCOGSDATA_LOG_LEVEL",
)
@click.option(
    "--log-file",
    help="Python log output file",
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
    envvar="DISCOGSDATA_LOG_FILE",
    show_default=True,
    default=None,
)
def cli(log_format, log_level, log_file):
    "CLI for exploring/exploiting a DB populated from Discogs Data"

    logging_config(log_format, log_level, log_file)


@cli.group(name="fabric")
def fabric():
    pass


@fabric.command("release")
@click.argument("number", type=click.INT)
def release(number):
    "Retrieve information regarding release Fabric release NUMBER"

    logging.info("Extracting information for release %d", number)

    conn = psycopg2.connect("")
    cur = conn.cursor()
    cur.execute(
        """
    select fabric_releases.release_id, release.title 
    from fabric_releases, release 
    where num=%s and release_id is not null and release.id = release_id
    """,
        (number,),
    )
    for row in cur.fetchall():
        print(row)
    conn.close()


@cli.group(name="fabriclive")
def fabriclive():
    pass


@fabriclive.command("release")
@click.argument("number", type=click.INT)
def release(number):
    "Retrieve information regarding release Fabric release NUMBER"
