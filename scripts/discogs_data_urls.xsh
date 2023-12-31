#!/usr/bin/env xonsh --no-rc
import re
import sys
import json
import email.utils

from datetime import datetime
from sqlite_utils.db import Database
from sqlite_utils.utils import TypeTracker

from requests_html import HTMLSession

DATA_URL_RGX = re.compile(r'.*?(?P<url_year>\d{4})(?P<url_month>\d{2})(?P<url_day>\d{2})_(?P<url_type>\w+)((\.xml\.gz$)|(\.txt))')

if __name__ == "__main__":
    print(f"Command line args: {$ARGS}")

    fieldnames = ["year", "url", "content_length", "metadata"]
    session = HTMLSession()

    urls = []

    for year in range(8, 24):
        url = f"https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html?prefix=data/20{year:02}"
        resp = session.get(url)
        # Discogs data pages employ embedded JavaScript to fully render
        # this gives the requests_html engine enough time to complete
        resp.html.render(sleep=1)
        urls.extend(
            [
                (2000 + year, l)
                for l in resp.html.links
                if l.endswith(".xml.gz") or l.endswith("_CHECKSUM.txt")
            ]
        )

    urls.sort()
    print(f"{len(urls)} in total", file=sys.stderr)

    url_rows = []
    for url in urls:
        clength = -1
        full_url = "http:" + url[1]
        metadata = {}
    	for i, line in enumerate( !(http --headers HEAD @(full_url) )):
            # Skip the first line which is the HTTP response ine for HEAD request
            if not (i and line.strip()): continue
            h, v = line.strip().split(maxsplit=1)
            metadata[h[:-1]] = v
            if h.startswith("Content-Length"):
                clength = int(v)

        probe_date = metadata.get("Date", None)
        probe_date_iso = email.utils.parsedate_to_datetime(probe_date).isoformat() if probe_date else None

        row = { "year": url[0],
                "url": f"http:{url[1]}",
                "content_length": clength,
                "http_metadata": metadata,
                "probe_date": probe_date_iso,
               }

	m = DATA_URL_RGX.search(full_url)
	if m:
	   row.update(m.groupdict())
        else:
           row.update({'url_year': None, 'url_month': None, 'url_day': None, 'url_type': None})

        row['url_type'] = row['url_type'].lower()

        url_rows.append(row)
        
        json.dump(row, sys.stdout, indent=None)
        sys.stdout.write("\n")

    # Equivalent to
    # sqlite-utils insert $ARGS[1] dataurls discogs_data_urls.jsonl --nl

    if len($ARGS) > 1:
        db_name, db_file = $ARGS[1], pf"{$ARGS[1]}"
        if db_file.exists():
            print(f"WARNING! db file {db_file}, already exists, not overwriting all existing data", file=sys.stderr)
            sys.exit(-1)
        db = Database($ARGS[1], recreate=True)
    else:
        db_name, db_file = "memory", ":memory:"
        db = Database(memory=True)

    print(f"Creating sqlite db: {db_name}", file=sys.stderr)

    tracker = TypeTracker()
    db['dataurls'].insert_all(tracker.wrap(url_rows))
    db['dataurls'].transform(types=tracker.types)
    res = next(db.query("select count(*) as rowcount from dataurls"))
    print(f"Total rows: {res['rowcount']}", file=sys.stderr)
