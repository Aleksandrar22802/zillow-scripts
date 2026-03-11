#!/usr/bin/env python3
import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple
import datetime as dt

from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

from common.data import TimeBucket


def coerce_zpid(stem: str):
    """Try to store zpid as int if purely numeric; otherwise keep as string."""
    try:
        return int(stem)
    except ValueError:
        return stem

def collect_documents(root: Path, batch_size: int) -> Tuple[int, int, List[Dict[str, Any]]]:
    """
    Walk the directory tree one level: each immediate subdir is a zipcode.
    Each *.json file in that subdir corresponds to a zpid.
    Returns (num_dirs, num_files, final_batch_buffer).
    """
    num_dirs = 0
    num_files = 0
    buffer: List[Dict[str, Any]] = []

    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        zipcode = entry.name.strip()
        if not zipcode:
            continue
        num_dirs += 1

        for json_path in sorted(entry.glob("*.json")):
            if not json_path.is_file():
                continue
            num_files += 1
            zpid = coerce_zpid(json_path.stem)

            try:
                with json_path.open("r", encoding="utf-8") as f:
                    contents = json.load(f)
            except json.JSONDecodeError as e:
                logging.warning("Skipping invalid JSON: %s (%s)", json_path, e)
                continue
            except OSError as e:
                logging.warning("Skipping unreadable file: %s (%s)", json_path, e)
                continue

            doc = {
                "zipcode": zipcode,
                "zpid": zpid,
                "contents": contents,
            }
            buffer.append(doc)

            # Flush in caller with insert_many to avoid duplicate code
            if len(buffer) >= batch_size:
                yield buffer
                buffer = []

    if buffer:
        yield buffer

    # Also return some stats (generators can’t return values pre-3.3 semantics; so do a second pass)
    # BUT we already yielded buffers; to keep it simple, recompute stats upfront and pass them separately
    # Instead, we’ll compute stats outside of the generator to keep clarity.
    # (Leaving this comment for clarity.)
    return

def main():
    parser = argparse.ArgumentParser(
        description="Load Zillow propertyExtendedSearch from filesystem into MongoDB."
    )
    parser.add_argument(
        "root",
        type=Path,
        help="Root directory containing subdirectories named by zipcode, each with <zpid>.json files.",
    )
    parser.add_argument(
        "--mongo-uri",
        default="mongodb://localhost:27017",
        help="MongoDB connection URI (default: mongodb://localhost:27017)",
    )
    parser.add_argument(
        "--db",
        default="zillow",
        help='Database name (default: "zillow")',
    )
    parser.add_argument(
        "--collection",
        default="propertyExtendedSearch",
        help='Collection name (default: "propertyExtendedSearch")',
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of documents per insert_many batch (default: 1000)",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop the collection before loading (DANGER: erases existing data).",
    )
    parser.add_argument(
        "--upsert",
        action="store_true",
        help="Upsert each document (slower; ensures updates). If not set, uses bulk insert and skips duplicates.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    root: Path = args.root
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Root path does not exist or is not a directory: {root}")

    client = MongoClient(args.mongo_uri)
    db = client[args.db]
    coll = db[args.collection]

    if args.drop:
        logging.warning('Dropping collection "%s.%s"...', args.db, args.collection)
        coll.drop()

    # Ensure a sensible unique key. Zipcodes can repeat across datasets, and zpids are typically unique
    # globally, but we’ll be conservative and enforce uniqueness on (zipcode, zpid).
    logging.info("Creating index on (zipcode, zpid)...")
    coll.create_index([("zipcode", ASCENDING), ("zpid", ASCENDING)], unique=True)

    # Precompute some basic stats for nice logs
    num_dirs = sum(1 for p in root.iterdir() if p.is_dir())
    num_files = sum(1 for d in root.iterdir() if d.is_dir() for _ in d.glob("*.json"))
    logging.info("Found %d zipcode directories containing %d JSON files.", num_dirs, num_files)

    inserted = 0
    skipped_dups = 0
    updated = 0

    if args.upsert:
        # Upsert one-by-one (safe but slower)
        for zipcode_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            zipcode = zipcode_dir.name.strip()
            for json_path in sorted(zipcode_dir.glob("*.json")):
                zpid = coerce_zpid(json_path.stem)
                try:
                    with json_path.open("r", encoding="utf-8") as f:
                        contents = json.load(f)
                except json.JSONDecodeError as e:
                    logging.warning("Skipping invalid JSON: %s (%s)", json_path, e)
                    continue
                except OSError as e:
                    logging.warning("Skipping unreadable file: %s (%s)", json_path, e)
                    continue

                res = coll.update_one(
                    {"zipcode": zipcode, "zpid": zpid},
                    {"$set": {"contents": contents}},
                    upsert=True,
                )
                if res.upserted_id is not None:
                    inserted += 1
                elif res.modified_count > 0:
                    updated += 1
        logging.info("Done. Inserted: %d | Updated: %d", inserted, updated)

    else:
        # Fast path: bulk insert in batches; on duplicates, skip them
        buffer: List[Dict[str, Any]] = []
        def flush(buf: List[Dict[str, Any]]):
            nonlocal inserted, skipped_dups
            if not buf:
                return
            try:
                result = coll.insert_many(buf, ordered=False)
                inserted += len(result.inserted_ids)
            except BulkWriteError as bwe:
                # Count inserted vs duplicate key errors
                write_errors = bwe.details.get("writeErrors", [])
                dup_count = sum(1 for e in write_errors if e.get("code") == 11000)
                skipped_dups += dup_count
                # Some docs may have been inserted even when errors occur
                inserted += bwe.details.get("nInserted", 0)

        for zipcode_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            zipcode = zipcode_dir.name.strip()
            for json_path in sorted(zipcode_dir.glob("*.json")):
                if not json_path.is_file():
                    continue
                try:
                    with json_path.open("r", encoding="utf-8") as f:
                        contents = json.load(f)
                except json.JSONDecodeError as e:
                    logging.warning("Skipping invalid JSON: %s (%s)", json_path, e)
                    continue
                except OSError as e:
                    logging.warning("Skipping unreadable file: %s (%s)", json_path, e)
                    continue


                scraped_at: dt.datetime = dt.datetime.now(dt.timezone.utc)
                time_bucket_id = TimeBucket.from_datetime(scraped_at).id
                contents_json = json.dumps(contents).encode("utf-8")
                doc = {
                    "zipcode": zipcode,
                    "zpid": coerce_zpid(json_path.stem),
                    "uri": contents.get("detailUrl", None),
                    "datetime": scraped_at,
                    "time_bucket_id": time_bucket_id,
                    "contents": contents,
                    "contentSize": len(contents_json),  # approximate size in bytes
                }
                buffer.append(doc)
                if len(buffer) >= args.batch_size:
                    flush(buffer)
                    buffer = []
        # final flush
        if buffer:
            flush(buffer)

        logging.info("Done. Inserted: %d | Duplicates skipped: %d", inserted, skipped_dups)

    logging.info("All set: %s.%s", args.db, args.collection)

if __name__ == "__main__":
    main()
