import logging

import click
import psycopg2

from .logconfig import DEFAULT_LOG_FORMAT, logging_config

FABRIC_RELEASE_QUERY = """
     select
     fabric_releases.fabric_num as fabric_num,
     fabric_releases.release_id,
     fabric_releases.title as release_title,
     concat_ws(', ', variadic array_agg(concat_ws(':', format('[%%s]', anv), format('[%%s]', artist_name)))) as release_artist,
     (array_agg(release.released))[1] as release_date,
     (array_agg(release.country))[1] as release_country
     from fabric_releases, fabric_release_artists, release
     where fabric_releases.release_id = fabric_release_artists.release_id
     and fabric_releases.release_id = release.id
     and fabric_releases.fabric_num = %s
     and fabric_releases.fabric_live = %s
     and fabric_releases.release_id not in (select release_id from fabric_release_blacklist)
     group by fabric_releases.release_id, fabric_releases.fabric_num, fabric_releases.title
"""

FABRIC_TRACK_QUERY = """
with release as (
     select
     fabric_releases.fabric_num as fabric_num,
     fabric_releases.release_id,
     title as release_title,
     concat_ws(', ', variadic array_agg(concat_ws(':', format('[%%s]', anv), format('[%%s]', artist_name)))) as release_artist
     from fabric_releases, fabric_release_artists
     where fabric_releases.release_id = fabric_release_artists.release_id
     and fabric_releases.fabric_num = %s
     and fabric_releases.fabric_live = %s
     and fabric_releases.release_id not in (select release_id from fabric_release_blacklist)
     group by fabric_releases.release_id, fabric_releases.fabric_num, fabric_releases.title
)
select release.*,
       fabric_tracks.track_id,
       fabric_tracks.track_sequence,
       fabric_tracks.track_position,
       fabric_tracks.track_title,
       concat_ws(' ', variadic fabric_tracks_artists.track_artists) as track_artists
from
release join fabric_tracks on (release.release_id = fabric_tracks.release_id)
left join fabric_tracks_artists on (fabric_tracks.track_id = fabric_tracks_artists.track_id)
order by release_id, track_sequence
"""


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


def fabric_release_info(fabric_num, live=False, headers=True):
    logging.info("Extracting release info for release %d", fabric_num)

    conn = psycopg2.connect("")
    cur = conn.cursor()

    cur.execute(
        FABRIC_RELEASE_QUERY,
        (fabric_num, live),
    )

    if headers:
        yield tuple((col.name for col in cur.description))

    if not cur.rowcount:
        logging.error("No info for fabric (%s) release: %s", live, number)
        conn.close()
        return

    for row in cur:
        yield row

    conn.close()


@fabric.command("release")
@click.option(
    "--live",
    help="Select from Fabric Live series",
    is_flag=True,
    type=bool,
    show_default=True,
    default=False,
)
@click.argument("fabricnums", nargs=-1, type=click.INT)
def release(fabricnums, live=False):
    """
    Retrieve information regarding Fabric release NUMBER
    """

    logging.info("Extracting release info for releases %s", fabricnums)

    if not fabricnums:
        logging.warn("No release numbers provided")

    for idx, fabricnum in enumerate(fabricnums):
        for row in fabric_release_info(fabricnum, live, not idx):
            print(row)

    return 0


def fabric_tracks_info(fabric_num, live=False, headers=True):
    logging.info("Extracting release info for release %d", fabric_num)

    conn = psycopg2.connect("")
    cur = conn.cursor()

    cur.execute(
        FABRIC_TRACK_QUERY,
        (fabric_num, live),
    )

    if headers:
        yield tuple((col.name for col in cur.description))

    if not cur.rowcount:
        logging.error("No tracks for fabric (%s) release: %s", live, number)
        conn.close()
        return

    for row in cur:
        yield row

    conn.close()


@fabric.command("tracks")
@click.option(
    "--live",
    help="Select from Fabric Live series",
    is_flag=True,
    type=bool,
    show_default=True,
    default=False,
)
@click.argument("fabricnums", nargs=-1, type=click.INT)
def tracks(fabricnums, live=False):
    """
    Retrieve information regarding tracks for Fabric release NUMBER
    """

    logging.info("Extracting tracks info for releases %s", fabricnums)

    if not fabricnums:
        logging.warn("No release numbers provided")

    for idx, fabricnum in enumerate(fabricnums):
        for row in fabric_tracks_info(fabricnum, live, not idx):
            print(row)

    return 0
