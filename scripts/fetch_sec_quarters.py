"""Download and extract SEC Financial Statement Data Set quarters.

The quarter list is taken from SEC's OWN listing page (verified, not assumed): every
<yyyy>q<n>.zip link found there is the corpus. Downloads are serial and polite (contact
User-Agent, 2 s spacing, exponential backoff), idempotent (skip if num.txt+sub.txt already
extracted), and atomic (.part rename). Only num.txt and sub.txt are extracted -- the
pipeline consumes nothing else.

Usage:
    python scripts/fetch_sec_quarters.py [--data-root C:/Users/hossa/dev/vantage-data]
                                         [--quarters 2009q1,2009q2] [--list-only]
"""

import argparse
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

LISTING_URL = "https://www.sec.gov/dera/data/financial-statement-data-sets"
ZIP_URL = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/{q}.zip"
USER_AGENT = "VANTAGE research hossain@pazooki.com"
MIN_FREE_BYTES = 100 * 1024**3  # 100 GB pre-flight; corpus footprint is estimated 50-60 GB
RETRY_DELAYS = [5, 25, 125]
POLITE_SLEEP_S = 2
WANTED_MEMBERS = ("num.txt", "sub.txt")


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_err = None
    for attempt, delay in enumerate([0] + RETRY_DELAYS):
        if delay:
            print(f"  retry {attempt}/{len(RETRY_DELAYS)} after {delay}s: {url}")
            time.sleep(delay)
        try:
            return urllib.request.urlopen(req, timeout=120)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise  # not transient: caller decides what a 404 means
            last_err = e
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
    raise last_err


def listed_quarters():
    """The authoritative quarter list, scraped from SEC's listing page."""
    html = http_get(LISTING_URL).read().decode("utf-8", errors="replace")
    quarters = sorted(set(re.findall(r"(\d{4}q[1-4])\.zip", html)))
    if not quarters:
        sys.exit("FATAL: no <yyyy>q<n>.zip links found on the SEC listing page; refusing to assume a range")
    return quarters


def already_extracted(source_dir: Path, q: str) -> bool:
    return all((source_dir / q / m).exists() and (source_dir / q / m).stat().st_size > 0 for m in WANTED_MEMBERS)


def download_zip(zips_dir: Path, q: str) -> Path:
    dest = zips_dir / f"{q}.zip"
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  zip cached: {dest.name}")
        return dest
    part = dest.with_suffix(".zip.part")
    with http_get(ZIP_URL.format(q=q)) as resp, open(part, "wb") as out:
        shutil.copyfileobj(resp, out)
    part.rename(dest)
    print(f"  downloaded: {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
    time.sleep(POLITE_SLEEP_S)
    return dest


def extract(zip_path: Path, source_dir: Path, q: str) -> None:
    qdir = source_dir / q
    qdir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        for member in WANTED_MEMBERS:
            if member not in names:
                sys.exit(f"FATAL: {zip_path.name} lacks {member} -- unexpected FSDS layout, stopping")
            with zf.open(member) as src, open(qdir / member, "wb") as out:
                shutil.copyfileobj(src, out)
    print(f"  extracted: {q}/num.txt + sub.txt")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="C:/Users/hossa/dev/vantage-data")
    ap.add_argument("--quarters", default="", help="comma list; default = all quarters on SEC's listing")
    ap.add_argument("--list-only", action="store_true", help="print the SEC-listed quarters and exit")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    source_dir = data_root / "source"
    zips_dir = data_root / "zips"
    zips_dir.mkdir(parents=True, exist_ok=True)

    free = shutil.disk_usage(data_root).free
    if free < MIN_FREE_BYTES:
        sys.exit(f"FATAL: {free / 1e9:.0f} GB free < {MIN_FREE_BYTES / 1e9:.0f} GB pre-flight floor")

    quarters = listed_quarters()
    print(f"SEC listing: {len(quarters)} quarters, earliest={quarters[0]} latest={quarters[-1]}")
    if args.list_only:
        return
    if args.quarters:
        wanted = [q.strip() for q in args.quarters.split(",") if q.strip()]
        unknown = sorted(set(wanted) - set(quarters))
        if unknown:
            sys.exit(f"FATAL: not on SEC's listing: {unknown}")
        quarters = wanted

    done, fetched, failed = 0, 0, []
    for q in quarters:
        if already_extracted(source_dir, q):
            done += 1
            continue
        print(f"[{q}]")
        try:
            extract(download_zip(zips_dir, q), source_dir, q)
            fetched += 1
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code} for {q} -- recorded as failed, continuing")
            failed.append(q)
    print(f"summary: {done} already present, {fetched} fetched, {len(failed)} failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
