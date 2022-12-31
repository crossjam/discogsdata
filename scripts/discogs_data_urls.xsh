#!/usr/bin/env xonsh
import sys

from requests_html import HTMLSession

if __name__ == "__main__":
    session = HTMLSession()

    urls = []

    for year in range(8, 23):
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
    	for line in !(http --headers HEAD @(furl)):
            if line.strip().startswith("Content-Length"):
                h, v = line.split()
                clength = int(v)
        print(f"{url[0]}, http:{url[1]}, {clength}")
