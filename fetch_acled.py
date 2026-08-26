#!/usr/bin/env python3
"""
fetch_acled.py — Conflict Atlas, Stage 5.2

Pulls recent conflict events from ACLED and writes data/live.json in the same
shape as data/incidents.json.

Runs on GitHub's servers via .github/workflows/update-live.yml.

Uses myACLED OAuth authentication. Requires ACLED_EMAIL and ACLED_PASSWORD
environment variables.
"""

import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

# =============================================================================
# CONFIG
# =============================================================================

DAYS_BACK = 90
MAX_EVENTS = 1000

OUTPUT_PATH = "data/live.json"

KEY_ID = "id"
KEY_DATE = "date"
KEY_TYPE = "type"
RECORDS_KEY = "incidents"

# ACLED event_type to our internal types
TYPE_MAPPING = {
    "Battles": "armed_clash",
    "Explosions/Remote violence": "terror_attack",
    "Violence against civilians": "armed_clash",
    "Protests": "civil_unrest",
    "Riots": "civil_unrest",
}

AUTH_URL = "https://acleddata.com/user/login?_format=json"
API_URL = "https://api.acleddata.com/acled/read/"

RETRIES = 3
RETRY_BACKOFF_SECONDS = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# =============================================================================

def log(msg):
    print(msg, flush=True)

def authenticate(email, password):
    log(f"Authenticating with ACLED as {email}...")
    req = urllib.request.Request(
        AUTH_URL,
        data=json.dumps({"name": email, "pass": password}).encode("utf-8"),
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Accept": "application/json"
        },
        method="POST"
    )
    
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                
                # Drupal / OAuth token paths
                if "access_token" in data:
                    return {"token": data["access_token"]}
                if "token" in data:
                    return {"token": data["token"]}
                if "csrf_token" in data and "logout_token" in data:
                    pass
                    
                log("Warning: could not find 'access_token' or 'token' in auth response. Using fallback session cookie auth if supported.")
                cookie = resp.headers.get('Set-Cookie')
                return {"cookie": cookie, "token": data.get("access_token", data.get("token"))}
                
        except urllib.error.HTTPError as e:
            if e.code == 401 or e.code == 403:
                raise SystemExit(f"Authentication failed: HTTP {e.code}. Check your credentials.")
            log(f"Auth HTTP {e.code} on attempt {attempt}")
            time.sleep(RETRY_BACKOFF_SECONDS)
        except Exception as e:
            log(f"Auth error: {e}")
            time.sleep(RETRY_BACKOFF_SECONDS)
            
    raise SystemExit("Could not authenticate with ACLED after multiple attempts.")


def fetch_events(auth_info):
    start_date = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    params = {
        "limit": 5000,
        "event_date": f"{start_date}|{end_date}",
        "event_date_where": "BETWEEN"
    }
    query_string = urllib.parse.urlencode(params)
    url = f"{API_URL}?{query_string}"
    
    log(f"Fetching data from {url}")
    
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    
    if isinstance(auth_info, dict):
        if auth_info.get("token"):
            headers["Authorization"] = f"Bearer {auth_info['token']}"
        if auth_info.get("cookie"):
            headers["Cookie"] = auth_info["cookie"]
            
    req = urllib.request.Request(url, headers=headers)
    
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if "data" in data:
                    return data["data"]
                return data
        except urllib.error.HTTPError as e:
            log(f"API HTTP {e.code} on attempt {attempt}")
            time.sleep(RETRY_BACKOFF_SECONDS)
        except Exception as e:
            log(f"API error: {e}")
            time.sleep(RETRY_BACKOFF_SECONDS)
            
    return []

def build_records(acled_rows):
    records = []
    skipped_type = skipped_coords = 0
    
    for row in acled_rows:
        try:
            event_type = row.get("event_type", "")
            mapped_type = TYPE_MAPPING.get(event_type)
            
            if not mapped_type:
                skipped_type += 1
                continue
                
            lat = float(row.get("latitude", 0))
            lon = float(row.get("longitude", 0))
            if lat == 0 and lon == 0:
                skipped_coords += 1
                continue
                
            fatalities = int(row.get("fatalities", 0))
            date_str = row.get("event_date", "")
            try:
                if " " in date_str:
                    date_str = date_str.split()[0]
            except Exception:
                pass
                
            city = row.get("location", "Unknown")
            region = row.get("admin1", "—")
            country = row.get("country", "Unknown")
            source = row.get("source", "ACLED")
            notes = row.get("notes", "No description provided.")
            
            records.append({
                KEY_ID: f"acled-{row.get('event_id_cnty', str(len(records)))}",
                KEY_DATE: date_str,
                "loc": [city, region, country, round(lat, 4), round(lon, 4)],
                KEY_TYPE: mapped_type,
                "cas": fatalities,
                "quality": "high",
                "ongoing": False,
                "desc": notes,
                "tags": ["live", "acled", country],
                "src": [s.strip() for s in source.split(";") if s.strip()],
                "source": "ACLED"
            })
        except Exception as e:
            skipped_coords += 1
            
    log(f"Filtered out: {skipped_type} unknown types, {skipped_coords} missing coordinates.")
    
    records = sorted(records, key=lambda r: r["cas"], reverse=True)
    if len(records) > MAX_EVENTS:
        log(f"Capping {len(records)} events at MAX_EVENTS={MAX_EVENTS}.")
        records = records[:MAX_EVENTS]
        
    return records


def main():
    email = os.environ.get("ACLED_EMAIL", "").strip()
    password = os.environ.get("ACLED_PASSWORD", "").strip()
    
    if not email or not password:
        raise SystemExit("ACLED_EMAIL and ACLED_PASSWORD environment variables are required.")
        
    auth_info = authenticate(email, password)
    acled_rows = fetch_events(auth_info)
    
    log(f"Fetched {len(acled_rows)} raw events from ACLED.")
    
    if not acled_rows:
        log("No records fetched. Exiting.")
        return
        
    records = build_records(acled_rows)
    
    payload = {
        "meta": {
            "source": "ACLED (Armed Conflict Location & Event Data Project)",
            "source_url": "https://acleddata.com/",
            "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "count": len(records),
            "caveat": "High-quality, human-curated conflict data provided by ACLED."
        },
        RECORDS_KEY: records,
    }
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        
    log(f"Wrote {len(records)} records to {OUTPUT_PATH}")
    log("Done.")


if __name__ == "__main__":
    main()
