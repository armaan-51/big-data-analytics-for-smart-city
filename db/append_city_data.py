"""Append newly generated multi-city sensor documents to existing collection.

SAFE: Does not drop or modify existing documents.
Adds indexes on 'city' and 'timestamp' if absent.

Usage:
  python db/append_city_data.py --uri mongodb://localhost:27017 --file data/city_sensor_data.json
"""

import argparse
from pathlib import Path
import json
from pymongo import MongoClient
from pymongo import errors as pymongo_errors

DB_NAME = "smart_city"
COLLECTION_NAME = "sensor_data"


def load_json_lines(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def ensure_indexes(coll):
    existing = coll.index_information().keys()
    if "city_1" not in existing:
        coll.create_index("city")
    if "timestamp_1" not in existing:
        coll.create_index("timestamp")


def append_data(uri: str, file_path: str):
    client = MongoClient(uri)
    db = client[DB_NAME]
    coll = db[COLLECTION_NAME]

    if not Path(file_path).exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    batch = []
    inserted = 0
    try:
        for doc in load_json_lines(file_path):
            batch.append(doc)
            if len(batch) >= 1000:
                coll.insert_many(batch)
                inserted += len(batch)
                batch.clear()
        if batch:
            coll.insert_many(batch)
            inserted += len(batch)
    except pymongo_errors.PyMongoError as e:
        print("Insertion error:", e)
        return

    ensure_indexes(coll)
    print(f"Appended {inserted} city documents. Total collection count: {coll.count_documents({})}")


def main():
    parser = argparse.ArgumentParser(description="Append multi-city data to MongoDB")
    parser.add_argument("--uri", type=str, default="mongodb://localhost:27017")
    parser.add_argument("--file", type=str, default="data/city_sensor_data.json")
    args = parser.parse_args()
    append_data(args.uri, args.file)


if __name__ == "__main__":
    main()
