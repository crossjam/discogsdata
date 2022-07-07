import csv
import json
import logging
import re
import sys

from copy import copy

import click
import psycopg2
from tabulate import tabulate

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

RE_DISCOGS_ALT_CANONICAL_NAME = re.compile(
    r"\[(?P<alt_name>[^]]*?)\]:\[(?P<canonical_name>[^]]*?)\]"
)


def extract_artist(re_match):
    if re_match.group("alt_name"):
        return re_match.group("alt_name")
    else:
        return re_match.group("canonical_name")


def convert_names(seq):
    return [
        (
            RE_DISCOGS_ALT_CANONICAL_NAME.sub(extract_artist, v)
            if isinstance(v, str)
            else v
        )
        for v in seq
    ]


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
@click.option("--fmt", "tablefmt", help="Table format", default="simple")
@click.pass_context
def cli(ctx, **kw):
    "CLI for exploring/exploiting a DB populated from Discogs Data"

    logging_config(kw["log_format"], kw["log_level"], kw["log_file"])
    ctx.obj = copy(kw)


@cli.group(name="fabric")
@click.option(
    "+live/-live", "fabriclive", help="Query FabricLive releases", default=False
)
def fabric(fabriclive):
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
@click.argument("fabricnums", nargs=-1, type=click.INT)
@click.pass_context
def release(ctx, fabricnums):
    """
    Retrieve information regarding Fabric release NUMBER
    """

    series = "fabriclive" if ctx.parent.params.get("fabriclive", False) else "fabric"

    logging.info(
        "Extracting release info from series %s, for releases %s", series, fabricnums
    )

    if not fabricnums:
        logging.warn("No release numbers provided")

    for idx, fabricnum in enumerate(fabricnums):
        for row in fabric_release_info(fabricnum, (series == "fabriclive"), not idx):
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
@click.argument("fabricnums", nargs=-1, type=click.INT)
@click.pass_context
def tracks(ctx, fabricnums, live=False):
    """
    Retrieve information regarding tracks for Fabric release NUMBER
    """

    series = "fabriclive" if ctx.parent.params.get("fabriclive", False) else "fabric"

    logging.info(
        "Extracting tracks info from series %s, for releases %s", series, fabricnums
    )

    logging.info("Extracting tracks info for releases %s", fabricnums)

    if not fabricnums:
        logging.warn("No release numbers provided")

    logging.info("tablefmt: %s", ctx.obj["tablefmt"])

    tablefmt = ctx.obj["tablefmt"]
    if tablefmt == "json":
        keys = []
        for idx, fabricnum in enumerate(fabricnums):
            for ridx, row in enumerate(
                fabric_tracks_info(fabricnum, (series == "fabriclive"), not idx)
            ):
                if not idx and not ridx:
                    hdrs = [
                        "fabric_series",
                    ] + list(row)
                else:
                    json.dump(
                        dict(zip(hdrs, [series] + convert_names(list(row)))), sys.stdout
                    )
                    print()
    elif tablefmt == "csv":
        writer = csv.writer(sys.stdout)

        for idx, fabricnum in enumerate(fabricnums):
            for ridx, row in enumerate(
                fabric_tracks_info(fabricnum, (series == "fabriclive"), not idx)
            ):
                if not idx and not ridx:
                    row = ["fabric_series"] + list(row)
                else:
                    row = [series] + convert_names(list(row))

                writer.writerow(row)
    else:
        hdrs = []
        rows = []
        for idx, fabricnum in enumerate(fabricnums):
            for ridx, row in enumerate(
                fabric_tracks_info(fabricnum, (series == "fabriclive"), not idx)
            ):
                if not idx and not ridx:
                    hdrs = ["fabric_series"] + list(row)
                else:
                    rows.append([series] + convert_names(list(row)))

        output_table = tabulate(
            rows,
            headers=hdrs,
            tablefmt=tablefmt,
        )
        click.echo(output_table)

    return 0
