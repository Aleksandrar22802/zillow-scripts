#!/usr/bin/env python3
import argparse
import csv
import gc
import json
import logging
import os
import re
import signal
import time
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple, Union, Iterable
from urllib.parse import quote_plus

from pymongo import MongoClient, ASCENDING

# ─────────────────────────── fast exit on Ctrl+C ────────────────────────────
def _sigint_immediate_exit(sig, frame):
    try:
        logging.error("Ctrl+C — exiting now.")
    except Exception:
        pass
    os._exit(130)

signal.signal(signal.SIGINT, _sigint_immediate_exit)

# ───────────────────────── helpers ─────────────────────────
def _as_list(maybe_iter: object) -> Iterable:
    """Return a safe iterable list; None/str/dict -> empty list, list/tuple -> itself."""
    if isinstance(maybe_iter, (list, tuple)):
        return maybe_iter
    return []

def try_parse_datetime_utc(s: Optional[str]) -> Optional[datetime]:
    """
    Best-effort parse. Returns a timezone-aware UTC datetime or None if invalid.
    Accepts ISO 8601 (with/without 'Z') or epoch seconds.
    """
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    # ISO 8601 with Z
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc)
    except Exception:
        pass
    # ISO 8601 without Z (maybe with offset)
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        pass
    # epoch seconds
    try:
        sec = float(s)
        return datetime.fromtimestamp(sec, tz=timezone.utc)
    except Exception:
        return None

def timebucket_id_from_datetime(dt: datetime) -> int:
    """Hourly time-bucket ID (UTC). One bucket = one hour since Unix epoch."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() // 3600)

def derive_default_uri(zpid: Union[int, str]) -> str:
    """Synthesize the default Zillow homedetails URL for a zpid."""
    return f"https://www.zillow.com/homedetails/{zpid}_zpid/"

def build_mongo_uri(base_uri: str, username: Optional[str], password: Optional[str]) -> str:
    """
    If username/password provided (env vars), inject them into base_uri safely (quote_plus).
    Works for mongodb:// and mongodb+srv://. If base_uri already has creds, env vars override.
    """
    if not username and not password:
        return base_uri

    scheme_sep = "://"
    i = base_uri.find(scheme_sep)
    if i == -1:
        # not a standard URI; just prepend creds
        userinfo = f"{quote_plus(username or '')}:{quote_plus(password or '')}@"
        return f"mongodb://{userinfo}{base_uri}"

    scheme = base_uri[:i]
    rest = base_uri[i + len(scheme_sep):]

    # find end of authority (first '/' or end)
    slash = rest.find("/")
    authority = rest if slash == -1 else rest[:slash]
    tail = "" if slash == -1 else rest[slash:]  # includes '/' and everything after

    # strip any existing creds
    hostpart = authority.split("@", 1)[-1]

    enc_user = quote_plus(username or "")
    enc_pw = quote_plus(password or "")
    userinfo = f"{enc_user}:{enc_pw}@" if (username or password) else ""

    return f"{scheme}{scheme_sep}{userinfo}{hostpart}{tail}"

# ───────────────────────── convert_to_schema (utf-8 size, robust) ────────────
def convert_to_schema(data):
    # ---- address (robust) ----
    address_obj = data.get("address") or {}
    address = address_obj.get("streetAddress") or ""
    match = re.match(r"(\d+)\s+(.*)", address)
    if match:
        street_number = match.group(1)
        street_name = match.group(2)
    else:
        street_number = ""
        street_name = address

    # ---- lot size (handle None / non-numeric) ----
    lotSize = data.get("lotSize", None)
    lot_size_sqft = None
    lot_size_acres = None
    if isinstance(lotSize, (int, float)) and lotSize > 0:
        lot_size_sqft = lotSize
        lot_size_acres = lotSize / 43560

    # ---- schools (handle missing or non-iterable) ----
    schools = _as_list(data.get("schools"))
    elementary_school = middle_school = high_school = None
    for idx, school in enumerate(schools):
        if not isinstance(school, dict):
            continue
        entry = {
            "name": school.get("name"),
            "rating": school.get("rating"),
            "distance": school.get("distance"),
            "grades": school.get("grades"),
            "type": "Public",
        }
        if idx == 0:
            elementary_school = entry
        elif idx == 1:
            middle_school = entry
        elif idx == 2:
            high_school = entry

    # ---- resoFacts & misc fallbacks ----
    reso = data.get("resoFacts") or {}
    attr = data.get("attributionInfo") or {}
    city = data.get("city")
    state = data.get("state")
    zipcode = data.get("zipcode")
    county = data.get("county")
    latitude = data.get("latitude")
    longitude = data.get("longitude")

    # hoa fees (robust: only int() if digits exist)
    hoa_raw = reso.get("hoaFee")
    hoa_fee = None
    if hoa_raw not in (None, ""):
        digits = re.sub(r"[^\d]", "", str(hoa_raw))
        hoa_fee = int(digits) if digits else None

    final_data = {
        "metadata": {
            "version": "1.0",
            "description": "Property data collection schema for real estate data",
            "collection_date": date.today().strftime("%Y-%m-%d"),
            "miner_hot_key": "5XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        },
        "ids": {
            "property": {
                "parcel_number": reso.get("parcelNumber"),
                "fips_code": data.get("countyFIPS")
            },
            "zillow": {"zpid": data.get("zpid")},
            "mls":   {"mls_number": attr.get("mlsId")}
        },
        "property": {
            "location": {
                "addresses": address,
                "street_number": street_number,
                "street_name": street_name,
                "unit_number": None,
                "city": city,
                "state": state,
                "zip_code": zipcode,
                "zip_code_plus_4": None,
                "county": county,
                "latitude": latitude,
                "longitude": longitude
            },
            "features": {
                "interior_features": reso.get("interiorFeatures"),
                "bedrooms": data.get("bedrooms"),
                "bathrooms": data.get("bathrooms"),
                "full_bathrooms": reso.get("bathroomsFull"),
                "half_bathrooms": reso.get("bathroomsHalf"),
                "exterior_features": reso.get("exteriorFeatures"),
                "garage_spaces": reso.get("garageParkingCapacity"),
                "total_parking_spaces": reso.get("parkingCapacity"),
                "pool": reso.get("poolFeatures"),
                "fireplace": reso.get("fireplaces"),
                "stories": reso.get("stories"),
                "hvac_type": {"heating": reso.get("heating"), "cooling": reso.get("cooling")},
                "flooring_type": reso.get("flooring")
            },
            "characteristics": {
                "property_type": data.get("homeType"),
                "property_subtype": reso.get("propertySubType"),
                "construction_material": reso.get("constructionMaterials"),
                "year_built": data.get("yearBuilt"),
                "year_renovated": None
            },
            "size": {
                "house_size_sqft": data.get("livingArea"),
                "lot_size_acres": lot_size_acres,
                "lot_size_sqft": lot_size_sqft
            },
            "utilities": {
                "sewer_type": reso.get("sewer"),
                "water_source": reso.get("waterSource"),
            },
            "school": {
                "elementary_school": elementary_school,
                "middle_school": middle_school,
                "high_school": high_school,
                "school_district": reso.get("highSchoolDistrict")
            },
            "hoa": {
                "hoa_fee_monthly": [{"date": date.today().strftime("%Y-%m-%d"), "value": hoa_fee}] if hoa_fee is not None else None,
                "hoa_fee_annual": [{"date": date.today().strftime("%Y-%m-%d"), "value": hoa_fee}] if hoa_fee is not None else None
            }
        }
    }

    valuation_data = {
        "assessment": {
            "assessor_tax_values": (
                [{"date": date.today().strftime("%Y-%m-%d"), "value": reso.get("taxAssessedValue")}]
                if reso.get("taxAssessedValue") else None
            ),
            "assessor_market_values": None
        }
    }

    market_data = {
        "zestimate_current": data.get("zestimate"),
        "zestimate_history": None,
    }

    price_per_sqft = []
    rent_estimate = []
    sales_history = []
    sold_history = []
    sold_flag = 0
    for p in _as_list(data.get("priceHistory")):
        if not isinstance(p, dict):
            continue
        try:
            p_date = p.get("date")
            if p_date and datetime.strptime(p_date, "%Y-%m-%d") < datetime(2022, 1, 1):
                continue
        except Exception:
            pass
        if p.get("pricePerSquareFoot") is not None:
            price_per_sqft.append({"date": p.get("date"), "value": p.get("pricePerSquareFoot")})
        if p.get("postingIsRental"):
            rent_estimate.append({"date": p.get("date"), "value": p.get("price")})
        if p.get("event") == "Sold":
            sales_history.append({
                "date": p.get("date"),
                "value": p.get("price"),
                "transaction_type": "sale",
                "source": p.get("source")
            })
            if sold_flag == 0:
                sold_flag = 1
                sold_history.append({"date": p.get("date"), "event": p.get("event"), "price": p.get("price")})
        elif sold_flag == 1:
            if str(p.get("event") or "").startswith("Listed for sale"):
                sold_flag = 2
            sold_history.append({"date": p.get("date"), "event": p.get("event"), "price": p.get("price")})

    market_data["price_per_sqft"] = price_per_sqft if price_per_sqft else None
    market_data["comparable_sales"] = None
    valuation_data["market"] = market_data
    valuation_data["rental"] = {"rent_estimate": rent_estimate if rent_estimate else None}
    final_data["valuation"] = valuation_data

    final_data["market_data"] = {
        "trends": {
            "days_on_market": (
                [{"date": date.today().strftime("%Y-%m-%d"), "value": data.get("daysOnZillow")}]
                if data.get("daysOnZillow") else None
            )
        }
    }
    final_data["home_sales"] = {"sales_history": sales_history if sales_history else None}

    if sold_history and sold_history[0].get("price") is None:
        sold_history[0]["price"] = sold_history[1]["price"] if len(sold_history) > 1 else None
    if data.get("price") == 0 and sold_history:
        data["price"] = sold_history[0]["price"]
    dateSold = sold_history[0]["date"] if sold_history else None

    sold_history.reverse()

    # Collect price changes into a list; set to None if empty
    price_changes: list = []
    for i, s in enumerate(sold_history):
        ev = str(s.get("event") or "")
        if ev.startswith("Price"):
            if i == 0:
                continue
            old_price = sold_history[i - 1].get("price")
            new_price = s.get("price")
            try:
                change_percent = ((new_price - old_price) / new_price * 100) if new_price else None
            except Exception:
                change_percent = None
            price_changes.append({
                "date": s.get("date"),
                "old_price": old_price,
                "new_price": new_price,
                "change_percent": change_percent
            })

    market_context_data = {
        "sale_date": dateSold,
        "final_sale_price": data.get("price"),
        "days_on_market": data.get("daysOnZillow"),
        "listing_timeline": sold_history if sold_history else None,
        "price_changes": price_changes or None
    }
    final_data["market_context"] = market_context_data

    final_data["neighborhood_context"] = {
        "recent_comparable_sales": None,
        "market_trends": {"median_sale_price_trend": None}
    }

    tax_assessment_data = {"current_assessment": reso.get("taxAssessedValue")}
    for t in _as_list(data.get("taxHistory")):
        if not isinstance(t, dict) or "time" not in t:
            continue
        try:
            dt = datetime.utcfromtimestamp(t["time"] / 1000)
        except Exception:
            continue
        if dt < datetime(2022, 1, 1):
            continue
        tax_assessment_data.setdefault("assessment_history", []).append({
            "date": dt.strftime("%Y-%m-%d"),
            "value": t.get("value")
        })
        tax_assessment_data.setdefault("annual_taxes", []).append({
            "year": dt.year,
            "amount": t.get("taxPaid")
        })
    final_data["tax_assessment"] = tax_assessment_data

    # SIZE: utf-8
    return final_data, len(json.dumps(final_data, indent=2).encode("utf-8", "ignore"))

# ─────────────────────────── utils / IO ───────────────────────────────
def configure_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

def ensure_statefile(path: Path):
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(["zipcode", "zpid", "fetchdate"])
        logging.info(f"Created empty state CSV: {path}")

def load_state(path: Path) -> Dict[Tuple[str, Union[int, str]], str]:
    """Returns { (zipcode,zpid) -> fetchdate }"""
    ensure_statefile(path)
    state: Dict[Tuple[str, Union[int, str]], str] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        first = True
        for row in r:
            if first:
                first = False
                if len(row) >= 3 and row[0].strip().lower() == "zipcode" and row[1].strip().lower() == "zpid":
                    continue  # header
            if not row or len(row) < 3:
                continue
            zipcode = row[0].strip()
            zpid_raw = row[1].strip()
            zpid = int(zpid_raw) if zpid_raw.isdigit() else zpid_raw
            fetchdate = row[2].strip()
            state[(zipcode, zpid)] = fetchdate
    logging.info(f"State CSV loaded: {len(state)} rows")
    return state

def atomic_write_state(path: Path, state: Dict[Tuple[str, Union[int, str]], str]):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["zipcode", "zpid", "fetchdate"])
        for (zipcode, zpid), fetchdate in state.items():
            w.writerow([zipcode, zpid, fetchdate])
    os.replace(tmp, path)
    logging.info(f"State CSV written: {len(state)} rows -> {path}")

def load_zpid_map(zpid_csv_path: Path) -> Dict[Union[int, str], Dict[str, Optional[str]]]:
    """
    Load zpid CSV (zpid, zipcode, lastSoldDate, uri) into a dict:
      { zpid: { "uri": <str or None>, "lastsold": <str or None> } }
    """
    zmap: Dict[Union[int, str], Dict[str, Optional[str]]] = {}
    if not zpid_csv_path.exists():
        logging.warning(f"ZPID CSV not found: {zpid_csv_path}")
        return zmap
    with zpid_csv_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        first = True
        for row in r:
            # Expecting 4 fields: zpid, zipcode, lastsold, uri
            if not row or len(row) < 4:
                continue
            if first:
                first = False
                if row[0].strip().lower() == "zpid":
                    continue
            zpid_raw = row[0].strip()
            lastsold = row[2].strip() if len(row) >= 3 else ""
            uri = row[3].strip() if len(row) >= 4 else ""
            zpid: Union[int, str] = int(zpid_raw) if zpid_raw.isdigit() else zpid_raw
            zmap[zpid] = {
                "uri": uri or None,
                "lastsold": lastsold or None
            }
    logging.info(f"ZPID map loaded: {len(zmap)} entries (uri + lastsold)")
    return zmap

def get_collection(mongo_uri: str, db: str, collection: str, drop: bool = False):
    # Read credentials ONLY from environment variables
    env_user = os.getenv("MONGO_USERNAME")
    env_pass = os.getenv("MONGO_PASSWORD")
    full_uri = build_mongo_uri(mongo_uri, env_user, env_pass)

    client = MongoClient(full_uri)
    coll = client[db][collection]
    if drop:
        logging.warning(f"Dropping collection '{db}.{collection}'")
        coll.drop()
    coll.create_index([("zipcode", ASCENDING), ("zpid", ASCENDING)], unique=True)
    return coll

# ─────────────────────────── core operations ──────────────────────────
def process_file(json_path: Path) -> Optional[Tuple[dict, str]]:
    """Returns (parsed_json, fetched_at) or None on failure."""
    try:
        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        fetched_at = str(data.get("_fetchedAt", "") or "")
        return data, fetched_at
    except Exception as e:
        logging.warning(f"Bad JSON {json_path}: {e}")
        return None

def upsert_replace(
    coll,
    zipcode: str,
    zpid: Union[int, str],
    raw: dict,
    mapped_uri: Optional[str],
    mapped_lastsold: Optional[str],
    fetched_at: str
):
    """
    REPLACE existing doc (or upsert) with:
      - 'uri'          : mapped value or synthesized default from zpid
      - 'label'        : 'zip:' + zipcode
      - 'content'      : converted schema
      - 'contentSize'  : utf-8 size of pretty JSON
      - 'datetime'     : precedence = lastSoldDate (CSV) → _fetchedAt (file) → now UTC
      - 'timeBucketId' : computed from 'datetime' (UTC, hourly)
    """
    content, content_size = convert_to_schema(raw)

    # Pick datetime with precedence rules
    dt_obj = (
        try_parse_datetime_utc(mapped_lastsold) or
        try_parse_datetime_utc(fetched_at) or
        datetime.utcnow().replace(tzinfo=timezone.utc)
    )
    dt_iso = dt_obj.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    time_bucket_id = timebucket_id_from_datetime(dt_obj)

    # URI fallback if missing
    final_uri = mapped_uri or derive_default_uri(zpid)

    doc = {
        "zipcode": zipcode,
        "zpid": zpid,
        "uri": final_uri,
        "label": f"zip:{zipcode}",
        "content": content,
        "contentSize": content_size,
        "datetime": dt_iso,
        "timeBucketId": time_bucket_id,
    }
    coll.replace_one({"zipcode": zipcode, "zpid": zpid}, doc, upsert=True)

def reconcile_once(
    state_csv: Path,
    data_dir: Path,
    zpid_csv: Path,
    mongo_uri: str,
    db_name: str,
    coll_name: str,
    max_files_per_run: Optional[int] = None,
    drop_collection: bool = False,
    purge_missing_files: bool = False,
    purge_missing_zpids: bool = False,
) -> Tuple[int, int, int, int]:
    """Returns (added, modified, deleted, unchanged)"""
    state = load_state(state_csv)
    remaining_for_deletion = set(state.keys())  # remove keys as we see them
    zmap = load_zpid_map(zpid_csv)  # zpid -> {uri, lastsold}
    coll = get_collection(mongo_uri, db_name, coll_name, drop=drop_collection)

    added = modified = deleted = unchanged = 0
    processed = 0

    if not data_dir.exists():
        logging.warning(f"Data dir missing: {data_dir}")
    else:
        for zipcode_dir in data_dir.iterdir():
            if not zipcode_dir.is_dir():
                continue
            zipcode = zipcode_dir.name
            for json_path in zipcode_dir.glob("*.json"):
                if max_files_per_run and processed >= max_files_per_run:
                    break

                zpid_name = json_path.stem
                zpid = int(zpid_name) if zpid_name.isdigit() else zpid_name
                key = (zipcode, zpid)

                parsed = process_file(json_path)
                if parsed is None:
                    continue
                raw, fetched_at = parsed

                prev = state.get(key)
                mapped = zmap.get(zpid, {})
                mapped_uri = mapped.get("uri")
                mapped_lastsold = mapped.get("lastsold")

                if prev is None:
                    try:
                        upsert_replace(coll, zipcode, zpid, raw, mapped_uri, mapped_lastsold, fetched_at)
                        state[key] = fetched_at
                        added += 1
                    except Exception as e:
                        logging.warning(f"Replace/insert failed {key}: {e}")
                else:
                    if key in remaining_for_deletion:
                        remaining_for_deletion.remove(key)
                    if (prev or "") != (fetched_at or ""):
                        try:
                            upsert_replace(coll, zipcode, zpid, raw, mapped_uri, mapped_lastsold, fetched_at)
                            state[key] = fetched_at
                            modified += 1
                        except Exception as e:
                            logging.warning(f"Replace/update failed {key}: {e}")
                    else:
                        unchanged += 1

                del raw
                processed += 1

            if max_files_per_run and processed >= max_files_per_run:
                break

    # Deletions for files removed from filesystem (OPTIONAL)
    if purge_missing_files and remaining_for_deletion:
        for key in list(remaining_for_deletion):
            zipcode, zpid = key
            try:
                coll.delete_one({"zipcode": zipcode, "zpid": zpid})
            except Exception as e:
                logging.warning(f"Delete failed {key}: {e}")
            state.pop(key, None)
            deleted += 1
        logging.warning(f"Purged {deleted} document(s) missing from filesystem/state.")

    # Purge documents whose zpid is NOT in the CSV (OPTIONAL)
    if purge_missing_zpids:
        try:
            valid_zpids = set(zmap.keys())
            current_zpids = coll.distinct("zpid")
            purged_docs = 0
            purged_zpids = 0
            for z in current_zpids:
                if z not in valid_zpids:
                    res = coll.delete_many({"zpid": z})
                    purged_docs += res.deleted_count
                    purged_zpids += 1
            if purged_docs:
                logging.warning(
                    f"Purged {purged_docs} docs for {purged_zpids} zpid(s) not found in zpid CSV (stale/invalid)."
                )
            else:
                logging.info("No stale documents by zpid (all zpids exist in zpid CSV).")
        except Exception as e:
            logging.error(f"Failed to purge invalid zpid docs: {e}")

    atomic_write_state(state_csv, state)
    gc.collect()

    logging.info(
        f"Summary: added={added} modified={modified} deleted={deleted} unchanged={unchanged}"
    )
    return added, modified, deleted, unchanged

# ─────────────────────────── CLI / main ───────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="Low-memory sync of MongoDB collection with a filesystem, tracked by a state CSV."
    )
    ap.add_argument("--state-csv", required=True, type=Path, help="State CSV (zipcode,zpid,fetchdate). Created if missing.")
    ap.add_argument("--data-dir", required=True, type=Path, help="Data root: /ROOT/<zipcode>/<zpid>.json")
    ap.add_argument("--zpid-csv", required=True, type=Path, help="ZPID CSV with columns: zpid,zipcode,lastsold,uri")
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--db", default="zillow")
    ap.add_argument("--collection", default="graphql")  # default per your request
    ap.add_argument("--drop-collection", action="store_true", help="Drop the target collection before syncing")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    ap.add_argument("--watch", action="store_true", help="Loop forever")
    ap.add_argument("--interval-seconds", type=float, default=30.0, help="Sleep between scans in --watch mode")
    ap.add_argument("--max-files-per-run", type=int, default=None, help="Optional cap of files processed per run (for huge trees)")
    # NEW: optional purges
    ap.add_argument("--purge-missing-files", action="store_true",
                    help="Also remove Mongo docs for items missing from filesystem/state CSV.")
    ap.add_argument("--purge-missing-zpids", action="store_true",
                    help="Also remove Mongo docs whose zpid is not present in the zpid CSV (stale).")
    args = ap.parse_args()

    logging.captureWarnings(True)
    configure_logging(args.log_level)

    first_pass = True
    while True:
        reconcile_once(
            state_csv=args.state_csv,
            data_dir=args.data_dir,
            zpid_csv=args.zpid_csv,
            mongo_uri=args.mongo_uri,
            db_name=args.db,
            coll_name=args.collection,
            max_files_per_run=args.max_files_per_run,
            drop_collection=args.drop_collection if first_pass else False,  # drop only once at start
            purge_missing_files=args.purge_missing_files,
            purge_missing_zpids=args.purge_missing_zpids,
        )
        first_pass = False
        if not args.watch:
            break
        time.sleep(max(0.1, args.interval_seconds))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.error("Interrupted — exiting.")
        os._exit(130)
