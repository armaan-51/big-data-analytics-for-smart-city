"""
Generate synthetic Smart City IoT sensor data in JSON format.
Creates at least 10,000 records with fields:
- sensor_id (string)
- area (string)
- timestamp (ISO format)
- noise_db (int)
- traffic_count (int)
- pm25 (int)

Usage (Windows PowerShell):
  python data/generate_sensor_data.py --output data/sensor_data.json --records 10000

The script is deterministic per --seed for reproducibility.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import argparse

AREAS = [
    "City Center",
    "Industrial Zone",
    "Residential Area",
    "Market Area",
    "Park Area",
]


def generate_record(base_time: datetime, areas: list[str]) -> dict:
    sensor_id = str(uuid.uuid4())
    area = random.choice(areas)

    # Simulate hour-of-day effects
    hour = random.randint(0, 23)
    ts = base_time + timedelta(minutes=random.randint(0, 60*24*7))  # within 7 days
    ts = ts.replace(hour=hour)

    # Traffic: higher in morning/evening peaks
    peak_multiplier = 1.6 if hour in (7, 8, 9, 17, 18, 19) else 1.0
    base_traffic = {
        "City Center": 80,
        "Industrial Zone": 60,
        "Residential Area": 40,
        "Market Area": 70,
        "Park Area": 20,
    }[area]
    traffic_count = int(random.gauss(base_traffic * peak_multiplier, 20))
    traffic_count = max(0, traffic_count)

    # Noise: correlated with traffic and area
    area_noise_bias = {
        "City Center": 70,
        "Industrial Zone": 75,
        "Residential Area": 50,
        "Market Area": 65,
        "Park Area": 45,
    }[area]
    noise_db = int(random.gauss(area_noise_bias + (traffic_count / 10), 8))
    noise_db = max(30, noise_db)

    # PM2.5: higher in Industrial and City areas, random spikes
    area_pm25_bias = {
        "City Center": 65,
        "Industrial Zone": 80,
        "Residential Area": 35,
        "Market Area": 55,
        "Park Area": 30,
    }[area]
    pm25 = int(random.gauss(area_pm25_bias, 15))
    if random.random() < 0.03:  # occasional pollution spike
        pm25 += random.randint(30, 80)
    pm25 = max(5, pm25)

    return {
        "sensor_id": sensor_id,
        "area": area,
        "timestamp": ts.isoformat(),
        "noise_db": noise_db,
        "traffic_count": traffic_count,
        "pm25": pm25,
    }


def generate_dataset(n_records: int, seed: int | None = None) -> list[dict]:
    if seed is not None:
        random.seed(seed)
    base_time = datetime.now().replace(minute=0, second=0, microsecond=0)
    return [generate_record(base_time, AREAS) for _ in range(n_records)]


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic sensor data")
    parser.add_argument("--output", type=str, default="data/sensor_data.json", help="Output JSON file path")
    parser.add_argument("--records", type=int, default=10000, help="Number of records to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)

    dataset = generate_dataset(args.records, seed=args.seed)
    with open(args.output, "w", encoding="utf-8") as f:
        for doc in dataset:
            f.write(json.dumps(doc) + "\n")  # JSON Lines format for easy Mongo import

    print(f"Generated {len(dataset)} records to {args.output}")


if __name__ == "__main__":
    main()
