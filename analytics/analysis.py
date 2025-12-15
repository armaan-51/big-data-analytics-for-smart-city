"""
Analytics using MongoDB aggregation pipelines for Smart City sensor data.

Provides functions to compute:
- Area-wise average PM2.5
- Traffic density by hour
- Average noise level by area
- Abnormal readings detection (noise_db > 85, traffic_count > 150, pm25 > 100)

These functions return lists of dictionaries suitable for visualization.
"""

from typing import List, Dict
from pymongo.collection import Collection


def area_wise_avg_pm25(coll: Collection) -> List[Dict]:
    pipeline = [
        {"$group": {"_id": "$area", "avg_pm25": {"$avg": "$pm25"}}},
        {"$project": {"_id": 0, "area": "$_id", "avg_pm25": {"$round": ["$avg_pm25", 2]}}},
        {"$sort": {"area": 1}},
    ]
    return list(coll.aggregate(pipeline))


def traffic_density_by_hour(coll: Collection) -> List[Dict]:
    pipeline = [
        {"$addFields": {"ts": {"$toDate": "$timestamp"}}},
        {"$group": {
            "_id": {"hour": {"$hour": "$ts"}},
            "avg_traffic": {"$avg": "$traffic_count"}
        }},
        {"$project": {"_id": 0, "hour": "$_id.hour", "avg_traffic": {"$round": ["$avg_traffic", 2]}}},
        {"$sort": {"hour": 1}},
    ]
    return list(coll.aggregate(pipeline))


def avg_noise_by_area(coll: Collection) -> List[Dict]:
    pipeline = [
        {"$group": {"_id": "$area", "avg_noise": {"$avg": "$noise_db"}}},
        {"$project": {"_id": 0, "area": "$_id", "avg_noise": {"$round": ["$avg_noise", 2]}}},
        {"$sort": {"area": 1}},
    ]
    return list(coll.aggregate(pipeline))


def abnormal_readings(coll: Collection) -> List[Dict]:
    pipeline = [
        {"$match": {
            "$or": [
                {"noise_db": {"$gt": 85}},
                {"traffic_count": {"$gt": 150}},
                {"pm25": {"$gt": 100}},
            ]
        }},
        {"$project": {
            "_id": 0,
            "sensor_id": 1,
            "area": 1,
            "timestamp": 1,
            "noise_db": 1,
            "traffic_count": 1,
            "pm25": 1,
        }},
        {"$sort": {"timestamp": 1}},
        {"$limit": 1000},  # cap for UI
    ]
    return list(coll.aggregate(pipeline))


def all_analytics(coll: Collection) -> Dict[str, List[Dict]]:
    return {
        "area_avg_pm25": area_wise_avg_pm25(coll),
        "traffic_by_hour": traffic_density_by_hour(coll),
        "noise_by_area": avg_noise_by_area(coll),
        "abnormal": abnormal_readings(coll),
    }
