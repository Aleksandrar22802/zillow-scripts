#!/usr/bin/env python3
import argparse
import logging
import os
from datetime import datetime
from urllib.parse import quote_plus

from pymongo import MongoClient, ASCENDING

def build_mongo_uri(base_uri, username, password):
    if not username and not password:
        return base_uri
    sep = "://"
    i = base_uri.find(sep)
    if i == -1:
        userinfo = f"{quote_plus(username or '')}:{quote_plus(password or '')}@"
        return f"mongodb://{userinfo}{base_uri}"
    scheme, rest = base_uri[:i], base_uri[i+len(sep):]
    slash = rest.find("/")
    authority = rest if slash == -1 else rest[:slash]
    tail = "" if slash == -1 else rest[slash:]
    hostpart = authority.split("@", 1)[-1]
    enc_user = quote_plus(username or "")
    enc_pw = quote_plus(password or "")
    userinfo = f"{enc_user}:{enc_pw}@" if (username or password) else ""
    return f"{scheme}{sep}{userinfo}{hostpart}{tail}"

def main():
    ap = argparse.ArgumentParser(
        description="Build compressed_index from graphql using a temporary collection and atomic rename."
    )
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--db", default="zillow")
    ap.add_argument("--source", default="graphql", help="Source data collection")
    ap.add_argument("--target", default="compressed_index", help="Final index collection")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    env_user = os.getenv("MONGO_USERNAME")
    env_pass = os.getenv("MONGO_PASSWORD")
    uri = build_mongo_uri(args.mongo_uri, env_user, env_pass)

    client = MongoClient(uri)
    db = client[args.db]
    src = db[args.source]

    # Temporary collection name (unique per run)
    tmp_name = f"{args.target}__tmp_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    tmp = db[tmp_name]

    # If a left-over temp collection exists, drop it first
    if tmp_name in db.list_collection_names():
        logging.warning(f"Found leftover temp collection {tmp_name}; dropping")
        tmp.drop()

    # Pipeline to compute (label -> arrays) into the TEMP collection using $out
    pipeline = [
        {
            "$match": {
                "label": {"$exists": True, "$ne": None},
                "timeBucketId": {"$exists": True, "$ne": None},
                "contentSize": {"$exists": True, "$ne": None},
            }
        },
        {
            "$group": {
                "_id": {"label": "$label", "timeBucketId": "$timeBucketId"},
                "size_sum": {"$sum": "$contentSize"},
            }
        },
        {"$sort": {"_id.label": 1, "_id.timeBucketId": 1}},
        {
            "$group": {
                "_id": "$_id.label",
                "time_bucket_ids": {"$push": "$_id.timeBucketId"},
                "sizes_bytes": {"$push": "$size_sum"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "label": "$_id",
                "time_bucket_ids": 1,
                "sizes_bytes": 1,
            }
        },
        {"$out": {"db": args.db, "coll": tmp_name}},
    ]

    logging.info(f"Starting aggregation into temp collection '{tmp_name}'…")
    src.aggregate(pipeline, allowDiskUse=True)
    tmp_docs = db[tmp_name].estimated_document_count()
    logging.info(f"Temp build complete. Temp docs: {tmp_docs}")

    # Create required indexes on the TEMP collection before rename
    logging.info("Creating indexes on temp collection…")
    tmp.create_index([("label", ASCENDING)], unique=True)

    # Atomic rename temp -> target (drop target if exists)
    logging.info(f"Atomically renaming '{tmp_name}' → '{args.target}' (drop target if exists)…")
    try:
        tmp.rename(args.target, dropTarget=True)
    except Exception as e:
        logging.error(f"Rename failed. Leaving temp collection '{tmp_name}' for inspection. Error: {e}")
        raise

    final_count = db[args.target].estimated_document_count()
    logging.info(f"compressed_index refresh complete. Final docs: {final_count}")

if __name__ == "__main__":
    main()
