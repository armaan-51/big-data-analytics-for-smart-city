"""
MongoDB setup: create database and collection, and insert data from JSON Lines.

Requirements:
- MongoDB server running locally or accessible via connection string
- pymongo installed

Usage:
  python db/mongo_setup.py --uri mongodb://localhost:27017 --file data/sensor_data.json

Creates:
- Database: smart_city
- Collection: sensor_data
"""

import argparse
from pathlib import Path
from pymongo import MongoClient
from pymongo import errors as pymongo_errors
import json
import sys

DB_NAME = "smart_city"
COLLECTION_NAME = "sensor_data"


def load_json_lines(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _connect_client(uri: str) -> MongoClient:
    """Create a MongoClient and ping the server; raise a helpful error if unavailable."""
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        # Force connection by pinging admin DB
        client.admin.command("ping")
        return client
    except pymongo_errors.ServerSelectionTimeoutError as e:
        print("Could not connect to MongoDB at URI:", uri)
        print("Error:", e)
        print(
            "\nHints:\n"
            "- If you don't have MongoDB locally, you can run it via Docker:\n"
            "    docker run --name mongo -p 27017:27017 -d mongodb/mongodb-community-server:7.0-ubuntu2204\n"
            "- Or use MongoDB Atlas (free): create a cluster and set MONGODB_URI to the connection string.\n"
        )
        sys.exit(1)


def setup_mongo(uri: str, file_path: str, batch_size: int = 5000):
    client = _connect_client(uri)
    db = client[DB_NAME]
    coll = db[COLLECTION_NAME]

    # Optional: drop existing collection for a clean run
    coll.drop()
    coll = db[COLLECTION_NAME]

    # Insert in batches for efficiency
    buffer = []
    inserted = 0
    try:
        for doc in load_json_lines(file_path):
            buffer.append(doc)
            if len(buffer) >= 1000:
                coll.insert_many(buffer)
                inserted += len(buffer)
                buffer.clear()
        if buffer:
            coll.insert_many(buffer)
            inserted += len(buffer)
    except pymongo_errors.PyMongoError as e:
        print("Insertion failed:", e)
        sys.exit(1)

    # Indexes to speed up aggregations
    coll.create_index("area")
    coll.create_index("timestamp")
    coll.create_index("traffic_count")
    coll.create_index("noise_db")
    coll.create_index("pm25")

    print(f"Inserted {inserted} documents into {DB_NAME}.{COLLECTION_NAME}")


def main():
    parser = argparse.ArgumentParser(description="MongoDB setup for smart city analytics")
    parser.add_argument("--uri", type=str, default="mongodb://localhost:27017", help="MongoDB connection URI")
    parser.add_argument("--file", type=str, default="data/sensor_data.json", help="Path to JSON Lines data file")
    args = parser.parse_args()

    if not Path(args.file).exists():
        raise FileNotFoundError(f"Data file not found: {args.file}")

    setup_mongo(args.uri, args.file)


if __name__ == "__main__":
    main()
