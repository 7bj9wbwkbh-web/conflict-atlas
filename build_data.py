#!/usr/bin/env python3
"""
build_data.py — turn UCDP conflict data into the file Conflict Atlas reads.

WHAT THIS DOES
    Reads UCDP Georeferenced Event Dataset (GED) records, either from a CSV
    you downloaded by hand or from the UCDP API, filters them down to a size
    a browser can actually draw, and writes data/incidents.json.

TWO WAYS TO RUN IT

    A) CSV mode — no token, no waiting. Download the GED CSV from
       https://ucdp.uu.se/downloads/ then:
           python build_data.py --csv GEDEvent_v26_1.csv

    B) API mode — needs a free UCDP token (request by email, 3-5 days):
           python build_data.py --api --token YOUR-TOKEN

Everything below the CONFIG block can be left alone.
"""

import argparse, json, os, re, sys, time
from collections import Counter

# ============================================================
# CONFIG — the only part you'll normally want to change
# ============================================================

YEAR_FROM   = 2020      # earliest year to include
YEAR_TO     = 2025      # latest year to include
MIN_DEATHS  = 10        # drop events below this death toll
MAX_EVENTS  = 2500      # hard cap; keeps the highest-fatality events
OUT_PATH    = "data/incidents.json"

# Why the cap: every incident becomes an SVG marker. The app is smooth to
# roughly 2,000-3,000 markers. Above that you need canvas rendering, which
# is a bigger change. Start here, raise it once you see how it feels.

API_BASE    = "https://ucdpapi.pcr.uu.se/api"
GED_VERSION = "26.1"    # check https://ucdp.uu.se/apidocs/ for the current one
PAGE_SIZE   = 1000

# UCDP type_of_violence -> the four incident types the app knows about.
#   1 = state-based armed conflict   (government involved)
#   2 = non-state conflict           (armed groups, no government)
#   3 = one-sided violence           (organised violence against civilians)
VIOLENCE_TYPE = {
    1: "armed_clash",
    2: "armed_clash",
    3: "terror_attack",   # the app labels this "Attack on civilians"
}
# NOTE: UCDP records lethal organised violence only. It has no protest or
# riot category, so nothing will map to "civil_unrest" or "border_dispute".
# Those come from ACLED, which is a separate (and licensed) source.


# ============================================================
# HELPERS
# ============================================================

def clean(value):
    """UCDP uses empty strings and the literal 'NULL' for missing data."""
    if value is None:
        return ""
    s = str(value).strip()
    return "" if s.upper() in ("", "NULL", "NA", "NONE", "-1") else s


def to_int(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_sources(source_article, limit=4):
    """
    UCDP packs citations into one string, entries separated by '///', each
    roughly 'Outlet Name, YYYY-MM-DD, Headline'. We want just the outlets.
    """
    text = clean(source_article)
    if not text:
        return []
    names = []
    for chunk in text.split("///"):
        outlet = chunk.split(",")[0].strip()
        outlet = re.sub(r"\s*\(.*?\)\s*$", "", outlet)      # drop trailing "(AFP)"
        if 2 < len(outlet) < 40 and outlet not in names:
            names.append(outlet)
        if len(names) >= limit:
            break
    return names


def assess_quality(row):
    """
    UCDP gives a low/best/high fatality range plus precision codes. A wide
    range or a fuzzy location means the number should not be trusted as exact.
    """
    best = to_int(row.get("best"))
    low, high = to_int(row.get("low")), to_int(row.get("high", best))
    clarity = to_int(row.get("event_clarity"), 1)
    where_prec = to_int(row.get("where_prec"), 1)

    spread = (high - low) / max(1, best)
    if spread == 0 and clarity == 1 and where_prec <= 2:
        return "high"
    if spread < 0.5 and where_prec <= 4:
        return "medium"
    return "low"


def describe(row):
    """A readable sentence, since UCDP has no free-text summary field."""
    side_a, side_b = clean(row.get("side_a")), clean(row.get("side_b"))
    place = clean(row.get("where_description")) or clean(row.get("where_coordinates"))
    conflict = clean(row.get("conflict_name"))
    deaths = to_int(row.get("best"))

    if side_a and side_b:
        who = f"{side_a} and {side_b}"
    else:
        who = side_a or side_b or "Unidentified armed actors"

    parts = [f"Recorded violence involving {who}"]
    if place:
        parts.append(f"at {place}")
    parts.append(f"— {deaths:,} death{'s' if deaths != 1 else ''} recorded by UCDP")
    sentence = " ".join(parts) + "."
    if conflict:
        sentence += f" Coded under the conflict '{conflict}'."
    return sentence


def normalize(row):
    """Turn one UCDP record into one Conflict Atlas record. Returns None to skip."""
    lat, lon = to_float(row.get("latitude")), to_float(row.get("longitude"))
    if lat is None or lon is None:
        return None

    date = clean(row.get("date_start"))[:10]
    if len(date) != 10:
        return None

    year = to_int(date[:4])
    if not (YEAR_FROM <= year <= YEAR_TO):
        return None

    deaths = to_int(row.get("best"))
    if deaths < MIN_DEATHS:
        return None

    vtype = VIOLENCE_TYPE.get(to_int(row.get("type_of_violence"), 1), "armed_clash")

    city = clean(row.get("where_coordinates")) or clean(row.get("adm_1")) or clean(row.get("country"))
    region = clean(row.get("adm_1")) or clean(row.get("region")) or "—"

    # UCDP dates an event with a start and an end. A multi-day span means
    # fighting continued, which is the closest thing it has to "ongoing".
    date_end = clean(row.get("date_end"))[:10]
    ongoing = bool(date_end and date_end != date)

    tags = [t for t in [clean(row.get("conflict_name")), clean(row.get("region")), str(year)] if t]

    return {
        "id":   f"ucdp-{clean(row.get('id')) or clean(row.get('relid'))}",
        "date": date,
        "loc":  [city, region, clean(row.get("country")) or "Unknown", round(lat, 4), round(lon, 4)],
        "type": vtype,
        "cas":  deaths,
        "quality": assess_quality(row),
        "ongoing": ongoing,
        "desc": describe(row),
        "tags": tags[:4],
        "src":  parse_sources(row.get("source_article")),
    }


# ============================================================
# LOADERS
# ============================================================

# Columns the script cannot work without. Checked up front so a bad file
# fails with a useful message instead of silently producing nothing.
REQUIRED_COLUMNS = ["date_start", "latitude", "longitude", "best",
                    "type_of_violence", "country"]


def check_columns(rows, where):
    """Fail loudly and helpfully if the file isn't what we expect."""
    if not rows:
        sys.exit(f"{where} contained no rows. Is the file empty, or still zipped?")

    found = set(rows[0].keys())
    missing = [c for c in REQUIRED_COLUMNS if c not in found]
    if missing:
        print(f"\nERROR: {where} is missing required columns: {', '.join(missing)}")
        print("\nColumns actually found:")
        for name in sorted(found)[:40]:
            print(f"    {name}")
        if len(found) > 40:
            print(f"    ... and {len(found)-40} more")
        print("\nLikely causes:")
        print("  - This isn't the GED event file. You want 'UCDP Georeferenced")
        print("    Event Dataset (GED) Global', not a country-year or conflict file.")
        print("  - The file is still a .zip. Unzip it first.")
        print("  - You pointed at the wrong file. Check the filename.")
        sys.exit(1)


def load_from_csv(path):
    import csv
    csv.field_size_limit(10_000_000)          # source_article can be enormous

    if not os.path.exists(path):
        sys.exit(f"No file at '{path}'.\n"
                 f"Check the name and that it's in this folder. Files here:\n  "
                 + "\n  ".join(sorted(f for f in os.listdir(".") if f.endswith((".csv", ".zip")))
                               or ["(no .csv or .zip files found)"]))

    if path.lower().endswith(".zip"):
        sys.exit(f"'{path}' is a zip archive. Double-click it to unzip, "
                 f"then point this script at the .csv inside.")

    print(f"Reading {path} ...")
    with open(path, newline="", encoding="utf-8-sig") as fh:
        # UCDP ships comma-separated, but exports and spreadsheet round-trips
        # can produce semicolons or tabs. Detect rather than assume.
        head = fh.read(64 * 1024)
        fh.seek(0)
        try:
            delimiter = csv.Sniffer().sniff(head, delimiters=",;\t|").delimiter
        except csv.Error:
            delimiter = ","
        if delimiter != ",":
            print(f"  (detected '{delimiter}' as the column separator)")

        reader = csv.DictReader(fh, delimiter=delimiter)
        # Headers are lowercased so a file exported with different casing
        # still works. UCDP's own variable names are already lowercase.
        rows = [{(k or "").strip().lower(): v for k, v in row.items()} for row in reader]

    check_columns(rows, path)
    return rows


def load_from_api(token):
    import urllib.request, urllib.parse
    rows, page, total_pages = [], 1, 1
    print("Fetching from the UCDP API ...")
    while page <= total_pages:
        query = urllib.parse.urlencode({
            "pagesize": PAGE_SIZE,
            "page": page,
            "StartDate": f"{YEAR_FROM}-01-01",
            "EndDate": f"{YEAR_TO}-12-31",
        })
        url = f"{API_BASE}/gedevents/{GED_VERSION}?{query}"
        request = urllib.request.Request(url, headers={"x-ucdp-access-token": token})
        try:
            with urllib.request.urlopen(request, timeout=90) as response:
                payload = json.load(response)
        except Exception as exc:
            print(f"  ! request failed on page {page}: {exc}")
            print("    Check your token, and that GED_VERSION is current.")
            sys.exit(1)

        total_pages = payload.get("TotalPages", 1)
        batch = payload.get("Result", [])
        if page == 1:
            check_columns(batch, "the UCDP API response")
        rows.extend(batch)
        print(f"  page {page}/{total_pages} — {len(rows):,} rows so far")
        page += 1
        time.sleep(0.4)                        # be polite; 5,000 requests/day cap
    return rows


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Build incidents.json from UCDP data.")
    parser.add_argument("--csv", help="path to a UCDP GED csv file")
    parser.add_argument("--api", action="store_true", help="fetch from the UCDP API instead")
    parser.add_argument("--token", default=os.environ.get("UCDP_TOKEN", ""),
                        help="UCDP API token (or set the UCDP_TOKEN environment variable)")
    parser.add_argument("--out", default=OUT_PATH)
    args = parser.parse_args()

    if args.api:
        if not args.token:
            sys.exit("No token. Pass --token YOUR-TOKEN or set UCDP_TOKEN.")
        raw = load_from_api(args.token)
    elif args.csv:
        raw = load_from_csv(args.csv)
    else:
        sys.exit("Choose one: --csv path/to/file.csv   or   --api --token YOUR-TOKEN")

    print(f"\nLoaded {len(raw):,} raw records.")

    incidents = [n for n in (normalize(r) for r in raw) if n]
    print(f"{len(incidents):,} passed the filters "
          f"(years {YEAR_FROM}-{YEAR_TO}, at least {MIN_DEATHS} deaths).")

    if len(incidents) > MAX_EVENTS:
        incidents.sort(key=lambda x: x["cas"], reverse=True)
        incidents = incidents[:MAX_EVENTS]
        cutoff = min(x["cas"] for x in incidents)
        print(f"Capped to the {MAX_EVENTS:,} deadliest (everything below "
              f"{cutoff:,} deaths dropped).")

    incidents.sort(key=lambda x: x["date"])

    if not incidents:
        # Work out WHY, so the message points at the real problem.
        years = sorted({str(r.get("date_start", ""))[:4] for r in raw if r.get("date_start")})
        years = [y for y in years if y.isdigit()]
        tolls = [to_int(r.get("best")) for r in raw]
        print("\nERROR: no events passed the filters. Diagnosing:")
        if years:
            print(f"  Years present in the file: {years[0]} to {years[-1]}")
            print(f"  Years you asked for:       {YEAR_FROM} to {YEAR_TO}")
            if int(years[-1]) < YEAR_FROM or int(years[0]) > YEAR_TO:
                print("  -> No overlap. Change YEAR_FROM / YEAR_TO in the CONFIG block.")
        else:
            print("  Could not read any dates. The date_start column may be malformed.")
        if tolls:
            print(f"  Highest death toll in the file: {max(tolls)}")
            print(f"  Your MIN_DEATHS setting:        {MIN_DEATHS}")
            if max(tolls) < MIN_DEATHS:
                print("  -> MIN_DEATHS is above every event. Lower it in the CONFIG block.")
        sys.exit(1)

    payload = {
        "source": f"UCDP GED {GED_VERSION}",
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "attribution": "Uppsala Conflict Data Program (UCDP), ucdp.uu.se",
        "filters": {"year_from": YEAR_FROM, "year_to": YEAR_TO,
                    "min_deaths": MIN_DEATHS, "max_events": MAX_EVENTS},
        "count": len(incidents),
        "incidents": incidents,
    }

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(args.out) / 1_048_576
    countries = Counter(i["loc"][2] for i in incidents)

    print(f"\nWrote {args.out} — {len(incidents):,} incidents, {size_mb:.2f} MB")
    print(f"Date range: {incidents[0]['date']} to {incidents[-1]['date']}")
    print(f"Total deaths: {sum(i['cas'] for i in incidents):,}")
    print("Top countries: " + ", ".join(f"{c} ({n})" for c, n in countries.most_common(5)))
    if size_mb > 5:
        print("\n! That file is large. Lower MAX_EVENTS or raise MIN_DEATHS "
              "so the page loads quickly.")


if __name__ == "__main__":
    main()
