#!/usr/bin/env python3
"""
fetch_live.py — Conflict Atlas, Stage 5.2

Pulls recent conflict events from GDELT and writes data/live.json in the same
shape as data/incidents.json.

Runs on GitHub's servers via .github/workflows/update-live.yml.
You never run this yourself.

Standard library only. No pip install, no requirements.txt.

------------------------------------------------------------------------------
WHY THIS DOESN'T USE THE GDELT API
------------------------------------------------------------------------------
Three GDELT endpoints came up while building this:

  /api/v2/doc/doc                 news articles. No coordinates. Can't map it.
  /api/v2/geo/geo                 GeoJSON with coordinates. Correct in
                                  principle — but returns 404 for long
                                  stretches, and did so for every request
                                  made while writing this.
  data.gdeltproject.org/gdeltv2/  the raw files. A static file server.
                                  New files every 15 minutes, at :00, :15,
                                  :30 and :45 past the hour.

This uses the third. It's the source the other two are built on, it's a plain
file host rather than a query service, and it has neither the rate limiting
nor the downtime of the API endpoints.

It's also a better fit. The raw Event table gives real event records —
coordinates, CAMEO event type, actor names, article counts — instead of
"places mentioned near a keyword." Your marker shapes finally have something
to key off.

------------------------------------------------------------------------------
WHAT THIS DATA IS AND ISN'T
------------------------------------------------------------------------------
UCDP gives you verified events with confirmed death tolls.
GDELT infers events from news coverage using an automated coder.

There is no death count here, because GDELT does not have one. The `deaths`
field is written as null on purpose. The severity score is derived from
article volume and CAMEO's Goldstein score — it reflects COVERAGE AND EVENT
TYPE, not casualties. Label it that way in the interface.
"""

import csv
import hashlib
import io
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone

# =============================================================================
# CONFIG
# =============================================================================

# How many 15-minute files to pull. 12 = the last 3 hours.
# Each file is small — tens to low hundreds of KB, zipped.
WINDOW_FILES = 12

# CAMEO event root codes to keep. This is the equivalent of the
# minimum-deaths filter in build_data.py — it's what makes this a conflict
# map rather than a news map.
#   18 = Assault
#   19 = Fight
#   20 = Use unconventional mass violence
# Add "17" (Coerce) if you also want abductions and property seizures.
KEEP_ROOT_CODES = {"18", "19", "20"}

# Require QuadClass 4 (Material Conflict). Set False to also allow QuadClass 3
# (Verbal Conflict) — threats and demands rather than acts.
MATERIAL_CONFLICT_ONLY = True

# Drop events reported by fewer than this many articles. Your noise filter.
# Raise it if the map looks like static.
MIN_ARTICLES = 2

# Hard ceiling on markers, same logic as Stage 1's Maximum events.
MAX_EVENTS = 800

OUTPUT_PATH = "data/live.json"
REFERENCE_PATH = "data/incidents.json"  # read only, to check key names match

# =============================================================================
# KEY NAMES — set these to match your incidents.json exactly
# =============================================================================
# The script prints the keys it finds in incidents.json when it runs, so the
# first run tells you if any of these are wrong. Change them here only.

KEY_ID = "id"
KEY_DATE = "date"
KEY_LAT = "lat"
KEY_LON = "lon"
KEY_COUNTRY = "country"
KEY_DEATHS = "deaths"
KEY_SEVERITY = "severity"
KEY_TYPE = "type"
KEY_LABEL = "label"

RECORDS_KEY = "incidents"

# CAMEO root code → the marker type your app draws. Match these strings to
# values already used in incidents.json.
TYPE_BY_ROOT_CODE = {
    "18": "Armed clash",
    "19": "Armed clash",
    "20": "Mass violence",
}

LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
FILE_URL_TEMPLATE = "http://data.gdeltproject.org/gdeltv2/%s.export.CSV.zip"

RETRIES = 4
RETRY_BACKOFF_SECONDS = 15
USER_AGENT = "conflict-atlas/1.0 (personal non-commercial project)"

# Column positions in the GDELT 2.0 Event table (61 columns, tab separated,
# no header row). Named rather than inlined so a format change is one edit.
COL_EVENT_ID = 0
COL_DAY = 1
COL_ACTOR1_NAME = 6
COL_ACTOR2_NAME = 16
COL_EVENT_ROOT_CODE = 28
COL_QUAD_CLASS = 29
COL_GOLDSTEIN = 30
COL_NUM_ARTICLES = 33
COL_AVG_TONE = 34
COL_ACTION_GEO_FULLNAME = 52
COL_ACTION_GEO_COUNTRY = 53
COL_ACTION_GEO_LAT = 56
COL_ACTION_GEO_LONG = 57
COL_SOURCE_URL = 60
EXPECTED_COLUMNS = 61


# =============================================================================


def log(msg):
    print(msg, flush=True)


def get(url, binary=False):
    """Fetch a URL with backoff. Returns bytes/str, or None if unavailable."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = resp.read()
            return data if binary else data.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None  # a missing 15-minute file; caller decides
            if e.code == 429 or 500 <= e.code < 600:
                if attempt == RETRIES:
                    log("  HTTP %s after %d attempts — skipping." % (e.code, RETRIES))
                    return None
                wait = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
                log("  HTTP %s, waiting %ds (%d/%d)." % (e.code, wait, attempt, RETRIES))
                time.sleep(wait)
                continue
            log("  HTTP %s — skipping." % e.code)
            return None
        except urllib.error.URLError as e:
            if attempt == RETRIES:
                log("  Unreachable (%s) — skipping." % e.reason)
                return None
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    return None


def latest_stamp():
    """
    Read lastupdate.txt and take the timestamp of the newest export file.
    The file is three lines of "size hash url"; we want the export one.
    """
    text = get(LASTUPDATE_URL)
    if not text:
        raise SystemExit("Could not read lastupdate.txt. Nothing to do this run.")

    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 3 and ".export.CSV.zip" in parts[2]:
            stamp = parts[2].rsplit("/", 1)[-1].split(".")[0]
            if len(stamp) == 14 and stamp.isdigit():
                log("Newest GDELT file: %s" % stamp)
                return stamp

    raise SystemExit("lastupdate.txt had no export entry. Format may have changed.")


def stamps_to_fetch(newest):
    """Walk backwards in 15-minute steps from the newest published file."""
    t = datetime.strptime(newest, "%Y%m%d%H%M%S")
    return [(t - timedelta(minutes=15 * i)).strftime("%Y%m%d%H%M%S")
            for i in range(WINDOW_FILES)]


def rows_from(stamp):
    """Download one 15-minute zip and return its rows. Missing files are fine."""
    blob = get(FILE_URL_TEMPLATE % stamp, binary=True)
    if not blob:
        return []

    try:
        zf = zipfile.ZipFile(io.BytesIO(blob))
        text = zf.read(zf.namelist()[0]).decode("utf-8", errors="replace")
    except (zipfile.BadZipFile, IndexError) as e:
        log("  %s: unreadable zip (%s) — skipping." % (stamp, e))
        return []

    return list(csv.reader(io.StringIO(text), delimiter="\t"))


def to_number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def severity_from(articles, goldstein):
    """
    0-10. NOT a casualty scale.

    Log on article volume, for the same reason the app defaults to
    SEVERITY_MODE = "log" — linear saturates immediately.

    Goldstein is CAMEO's conflict-intensity score, roughly -10 (worst) to +10.
    Everything here is already negative; this is what separates a shooting
    from a massacre.
    """
    base = math.log10(max(articles, 1) + 1) / math.log10(101) * 6.0  # 0-6
    if goldstein is None:
        weight = 2.0
    else:
        weight = max(0.0, min(4.0, (-goldstein) / 10.0 * 4.0))  # 0-4
    return round(min(10.0, base + weight), 1)


def build_records(all_rows):
    seen = {}
    skipped_type = skipped_articles = skipped_coords = skipped_malformed = 0

    for row in all_rows:
        if len(row) < EXPECTED_COLUMNS:
            skipped_malformed += 1
            continue

        root = row[COL_EVENT_ROOT_CODE].strip()
        if root not in KEEP_ROOT_CODES:
            skipped_type += 1
            continue

        if MATERIAL_CONFLICT_ONLY and row[COL_QUAD_CLASS].strip() != "4":
            skipped_type += 1
            continue

        lat = to_number(row[COL_ACTION_GEO_LAT])
        lon = to_number(row[COL_ACTION_GEO_LONG])
        if lat is None or lon is None or (lat == 0 and lon == 0):
            skipped_coords += 1
            continue

        articles = int(to_number(row[COL_NUM_ARTICLES]) or 0)
        if articles < MIN_ARTICLES:
            skipped_articles += 1
            continue

        event_id = row[COL_EVENT_ID].strip()
        # GDELT republishes an event as more coverage arrives. Keep the
        # version with the most articles.
        if event_id in seen and seen[event_id]["articles"] >= articles:
            continue

        try:
            date = datetime.strptime(row[COL_DAY].strip(), "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        place = row[COL_ACTION_GEO_FULLNAME].strip() or "Unknown location"
        # ActionGeo_FullName is "City, Region, Country". Take the readable
        # country name from the end of it — ActionGeo_CountryCode is a FIPS
        # code ("UP" for Ukraine, "BM" for Myanmar), which won't match the
        # country names in incidents.json. Fall back to the code only if the
        # name has no comma to split on.
        parts = [p.strip() for p in place.split(",") if p.strip()]
        country = parts[-1] if len(parts) > 1 else (
            row[COL_ACTION_GEO_COUNTRY].strip() or place)
        goldstein = to_number(row[COL_GOLDSTEIN])
        tone = to_number(row[COL_AVG_TONE])

        actors = [a.title() for a in
                  (row[COL_ACTOR1_NAME].strip(), row[COL_ACTOR2_NAME].strip()) if a]
        who = " vs ".join(actors) if len(actors) == 2 else (actors[0] if actors else "")
        label = "%s — %s" % (place, who) if who else place

        seen[event_id] = {
            KEY_ID: "gdelt-" + hashlib.md5(event_id.encode()).hexdigest()[:12],
            KEY_DATE: date,
            KEY_LAT: round(lat, 4),
            KEY_LON: round(lon, 4),
            KEY_COUNTRY: country,
            KEY_DEATHS: None,      # GDELT has no casualty figures. Deliberate.
            KEY_SEVERITY: severity_from(articles, goldstein),
            KEY_TYPE: TYPE_BY_ROOT_CODE.get(root, "Armed clash"),
            KEY_LABEL: label,
            # Extras the historical records don't have. Harmless if ignored.
            "articles": articles,
            "goldstein": goldstein,
            "tone": round(tone, 2) if tone is not None else None,
            "url": row[COL_SOURCE_URL].strip(),
            "source": "GDELT",
        }

    log("Filtered out: %d wrong event type, %d below MIN_ARTICLES=%d, "
        "%d without coordinates, %d malformed."
        % (skipped_type, skipped_articles, MIN_ARTICLES,
           skipped_coords, skipped_malformed))

    records = sorted(seen.values(), key=lambda r: r[KEY_SEVERITY], reverse=True)
    if len(records) > MAX_EVENTS:
        log("Capping %d events at MAX_EVENTS=%d (keeping the most severe)."
            % (len(records), MAX_EVENTS))
        records = records[:MAX_EVENTS]
    return records


def check_against_incidents(records):
    """
    Compares key names against the historical file. Modifies nothing — it just
    reports a disagreement, which is the likeliest reason the Live tab would
    render blank.
    """
    if not os.path.exists(REFERENCE_PATH):
        log("No %s to compare against — skipping the key-name check." % REFERENCE_PATH)
        return

    try:
        with open(REFERENCE_PATH, "r", encoding="utf-8") as f:
            ref = json.load(f)
    except Exception as e:
        log("Could not read %s (%s) — skipping the key-name check." % (REFERENCE_PATH, e))
        return

    ref_records = ref.get(RECORDS_KEY) if isinstance(ref, dict) else ref
    if not isinstance(ref_records, list) or not ref_records:
        log("Could not find a record array under '%s' in %s." % (RECORDS_KEY, REFERENCE_PATH))
        log("Set RECORDS_KEY at the top of this file to whatever it actually is.")
        return

    ref_keys = set(ref_records[0].keys())
    log("Keys in incidents.json: %s" % ", ".join(sorted(ref_keys)))

    if not records:
        return

    missing = ref_keys - set(records[0].keys())
    if missing:
        log("WARNING — incidents.json has keys live.json does not: %s"
            % ", ".join(sorted(missing)))
        log("If the Live tab renders blank, this is almost certainly why.")
        log("Fix the KEY_* values near the top of this file.")
    else:
        log("Key names line up with incidents.json.")

    ref_types = {r.get(KEY_TYPE) for r in ref_records if r.get(KEY_TYPE)}
    unknown = set(TYPE_BY_ROOT_CODE.values()) - ref_types
    if ref_types and unknown:
        log("NOTE — these marker types aren't used in incidents.json: %s"
            % ", ".join(sorted(unknown)))
        log("Types your app already knows: %s" % ", ".join(sorted(ref_types)))
        log("Adjust TYPE_BY_ROOT_CODE if the markers draw wrong.")


def main():
    newest = latest_stamp()
    stamps = stamps_to_fetch(newest)
    log("Fetching %d files (%.1f hours of coverage) …"
        % (len(stamps), len(stamps) * 15 / 60))

    all_rows = []
    fetched = 0
    for stamp in stamps:
        rows = rows_from(stamp)
        if rows:
            fetched += 1
            all_rows.extend(rows)

    log("Got %d of %d files, %d raw event rows." % (fetched, len(stamps), len(all_rows)))

    if fetched == 0:
        log("No files retrieved. Leaving the existing live.json alone.")
        raise SystemExit(0)

    records = build_records(all_rows)
    log("%d conflict events after filtering." % len(records))

    check_against_incidents(records)

    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                if json.load(f).get(RECORDS_KEY) == records:
                    log("Identical to the current %s. Leaving it untouched." % OUTPUT_PATH)
                    log("Done.")
                    return
        except Exception:
            pass

    payload = {
        "meta": {
            "source": "GDELT 2.0 Event Database",
            "source_url": "https://www.gdeltproject.org/data.html",
            "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "newest_file": newest,
            "window_hours": round(len(stamps) * 15 / 60, 1),
            "count": len(records),
            "caveat": ("Automatically coded from news coverage. Unverified, "
                       "includes false positives. Severity reflects coverage "
                       "volume and event type, not casualties. Not comparable "
                       "to UCDP figures."),
        },
        RECORDS_KEY: records,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    log("Wrote %d records to %s" % (len(records), OUTPUT_PATH))
    for r in records[:5]:
        log("  %.1f  %s" % (r[KEY_SEVERITY], r[KEY_LABEL]))
    log("Done.")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        log("Failed: %s: %s" % (type(e).__name__, e))
        sys.exit(1)
