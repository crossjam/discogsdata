#!/usr/bin/env xonsh
import sys
import json

from requests_html import HTMLSession

if __name__ == "__main__":
    fieldnames = ["year", "url", "content_length", "metadata"]
    session = HTMLSession()

    urls = []

    for year in range(8, 24):
        url = f"https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html?prefix=data/20{year:02}"
        resp = session.get(url)
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

    for i, url in enumerate(urls):
        clength = -1
        furl = "http:" + url[1]
        metadata = {}
    	for i, line in enumerate( !(http --headers HEAD @(furl) )):
            if not (i and line.strip()): continue
            # print(line.strip())
            h, v = line.strip().split(maxsplit=1)
            metadata[h[:-1]] = v
            if h.startswith("Content-Length"):
                clength = int(v)

        row = { "year": url[0],
                "url": f"http:{url[1]}",
                "content_length": clength,
                "metadata": metadata,
               }
        json.dump(row, sys.stdout, indent=None)
        sys.stdout.write("\n")


