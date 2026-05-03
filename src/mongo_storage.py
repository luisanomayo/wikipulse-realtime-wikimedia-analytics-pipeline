from datetime import datetime, timezone

import certifi
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from config import MONGODB_URI, MONGODB_DATABASE


def get_mongo_collections():
    client = MongoClient(
        MONGODB_URI,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=10000,
    )

    database = client[MONGODB_DATABASE]

    raw_events_collection = database["raw_events"]
    processed_events_collection = database["processed_events"]
    rejected_events_collection = database["rejected_events"]

    return {
        "raw": raw_events_collection,
        "processed": processed_events_collection,
        "rejected": rejected_events_collection,
    }


def create_storage_indexes(collections):
    collections["raw"].create_index("event_id", unique=True)
    collections["raw"].create_index("ingested_at")

    collections["processed"].create_index("event_id", unique=True)
    collections["processed"].create_index("event_timestamp")
    collections["processed"].create_index([("page_key", 1), ("event_timestamp", -1)])
    collections["processed"].create_index([("user", 1), ("event_timestamp", -1)])
    collections["processed"].create_index([("is_bot", 1), ("event_timestamp", -1)])
    collections["processed"].create_index([("event_type", 1), ("event_timestamp", -1)])
    collections["processed"].create_index([("wiki", 1), ("event_timestamp", -1)])
    collections["processed"].create_index([("language_code", 1), ("event_timestamp", -1)])
    collections["processed"].create_index([("server_name", 1), ("event_timestamp", -1)])

    collections["rejected"].create_index("reason")
    collections["rejected"].create_index("ingested_at")


def insert_one_safely(collection, document):
    try:
        collection.insert_one(document)
        return "inserted"

    except DuplicateKeyError:
        return "duplicate"


def store_processed_event(collections, raw_event, processed_event):
    raw_document = {
        "event_id": processed_event["event_id"],
        "ingested_at": processed_event["ingested_at"],
        "raw_event": raw_event,
    }

    raw_result = insert_one_safely(collections["raw"], raw_document)
    processed_result = insert_one_safely(collections["processed"], processed_event)

    return {
        "raw": raw_result,
        "processed": processed_result,
    }


def store_rejected_event(collections, rejected_event):
    rejected_event["ingested_at"] = rejected_event.get(
        "ingested_at",
        datetime.now(timezone.utc).isoformat(),
    )

    return insert_one_safely(collections["rejected"], rejected_event)