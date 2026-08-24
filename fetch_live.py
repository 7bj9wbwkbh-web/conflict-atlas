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

There is no death count here, because GDELT does not have one. `cas` is
always written as 0 — see the schema note below for why that's 0 and not
null. The severity score is derived from article volume and CAMEO's
Goldstein score — it reflects COVERAGE AND EVENT TYPE, not casualties.
Every record's `desc` says this explicitly, and `quality` is always "low".

------------------------------------------------------------------------------
2026-08 FIX — the output schema didn't match incidents.json
------------------------------------------------------------------------------
The original version of this script wrote flat top-level lat/lon/country
fields, a `deaths` key, and human-readable type strings ("Armed clash").
conflict-atlas.html's ingest() expects a nested `loc` array
([city, region, country, lat, lon]), a `cas` key, and type values from a
fixed set (armed_clash / terror_attack / border_dispute / civil_unrest) —
anything else crashes on `TYPES[r.type].label` being undefined, which is
why the Live tab rendered blank despite this workflow showing all-green runs.
build_records() below now emits the corrected shape directly. The KEY_*
constants section reflects what's actually required rather than a flat
per-field guess.
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
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone

# =============================================================================
# CONFIG
# =============================================================================

WINDOW_FILES = 12
KEEP_ROOT_CODES = {"18", "19", "20"}
MATERIAL_CONFLICT_ONLY = True
MIN_ARTICLES = 2
MAX_EVENTS = 800

OUTPUT_PATH = "data/live.json"
REFERENCE_PATH = "data/incidents.json"

# =============================================================================
# KEY NAMES — must match conflict-atlas.html's incidents.json schema exactly
# =============================================================================
# Confirmed against a real incidents.json: every record has these top-level
# keys — id, date, loc, type, cas, quality, ongoing, desc, tags, src — where
# `loc` is an array [city, region, country, lat, lon], not separate fields.
# That array packing is why flat KEY_LAT/KEY_LON/KEY_COUNTRY constants never
# worked here: the app never looks at those, only at r.loc[3] / r.loc[4].

KEY_ID = "id"
KEY_DATE = "date"
KEY_TYPE = "type"
KEY_SEVERITY = "severity"   # NOT a key in incidents.json — index.html's
                             # ingest() uses this only when present, in place
                             # of recomputing severity from casualties, since
                             # GDELT records always have cas=0.

RECORDS_KEY = "incidents"

# CAMEO root code -> the marker type conflict-atlas.html actually recognizes.
# These MUST be one of: armed_clash, terror_attack, border_dispute,
# civil_unrest — any other string crashes ingest() on TYPES[r.type].label.
TYPE_BY_ROOT_CODE = {
    "18": "armed_clash",     # Assault
    "19": "armed_clash",     # Fight
    "20": "terror_attack",   # Use unconventional mass violence
}

LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
FILE_URL_TEMPLATE = "http://data.gdeltproject.org/gdeltv2/%s.export.CSV.zip"

RETRIES = 4
RETRY_BACKOFF_SECONDS = 15
USER_AGENT = "conflict-atlas/1.0 (personal non-commercial project)"

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
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = resp.read()
            return data if binary else data.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
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
    t = datetime.strptime(newest, "%Y%m%d%H%M%S")
    return [(t - timedelta(minutes=15 * i)).strftime("%Y%m%d%H%M%S")
            for i in range(WINDOW_FILES)]


def rows_from(stamp):
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


def domain_from_url(url):
    """Bare domain for a source chip — 'reuters.com', not the full URL."""
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


def place_parts(full_name, country_code_fallback):
    """
    ActionGeo_FullName is "City, Region, Country" at best, or just a country
    name at worst. Split it into the three pieces conflict-atlas.html's
    `loc` array wants: [city, region, country].
    """
    parts = [p.strip() for p in full_name.split(",") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[-2], parts[-1]
    if len(parts) == 2:
        return parts[0], "—", parts[1]
    if len(parts) == 1:
        return "—", "—", parts[0]
    return "Unknown location", "—", (country_code_fallback or "Unknown")


def severity_from(articles, goldstein):
    """
    0-10. NOT a casualty scale.

    Log on article volume, for the same reason the app defaults to
    SEVERITY_MODE = "log" — linear saturates immediately.

    Goldstein is CAMEO's conflict-intensity score, roughly -10 (worst) to +10.
    Everything here is already negative; this is what separates a shooting
    from a massacre.
    """
    base = math.log10(max(articles, 1) + 1) / math.log10(101) * 6.0
    if goldstein is None:
        weight = 2.0
    else:
        weight = max(0.0, min(4.0, (-goldstein) / 10.0 * 4.0))
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
        if event_id in seen and seen[event_id]["articles"] >= articles:
            continue

        try:
            date = datetime.strptime(row[COL_DAY].strip(), "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        place = row[COL_ACTION_GEO_FULLNAME].strip() or "Unknown location"
        city, region, country = place_parts(place, row[COL_ACTION_GEO_COUNTRY].strip())
        goldstein = to_number(row[COL_GOLDSTEIN])
        tone = to_number(row[COL_AVG_TONE])
        sev = severity_from(articles, goldstein)

        actors = [a.title() for a in
                  (row[COL_ACTOR1_NAME].strip(), row[COL_ACTOR2_NAME].strip()) if a]
        who = " vs ".join(actors) if len(actors) == 2 else (actors[0] if actors else "")
        label = "%s — %s" % (place, who) if who else place

        event_type = TYPE_BY_ROOT_CODE.get(root, "armed_clash")
        source_url = row[COL_SOURCE_URL].strip()
        domain = domain_from_url(source_url)

        seen[event_id] = {
            KEY_ID: "gdelt-" + hashlib.md5(event_id.encode()).hexdigest()[:12],
            KEY_DATE: date,
            "loc": [city, region, country, round(lat, 4), round(lon, 4)],
            KEY_TYPE: event_type,
            "cas": 0,
            "quality": "low",
            "ongoing": False,
            "desc": ("%s — coded from %d article%s of coverage. Automated, "
                      "unverified; not a confirmed incident record."
                      % (label, articles, "" if articles == 1 else "s")),
            "tags": ["live", "gdelt", country],
            "src": [domain] if domain else [],
            KEY_SEVERITY: sev,
            "articles": articles,
            "goldstein": goldstein,
            "tone": round(tone, 2) if tone is not None else None,
            "url": source_url,
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
        log("  %.1f  %s" % (r[KEY_SEVERITY], r["desc"][:60]))
    log("Done.")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        log("Failed: %s: %s" % (type(e).__name__, e))
        sys.exit(1)
