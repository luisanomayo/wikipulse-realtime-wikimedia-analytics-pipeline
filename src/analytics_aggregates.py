import certifi
from pymongo import MongoClient, UpdateOne

from config import MONGODB_URI, MONGODB_DATABASE, validate_config


def get_database():
    client = MongoClient(
        MONGODB_URI,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=10000,
    )

    return client[MONGODB_DATABASE]


def upsert_aggregate_documents(collection, documents, unique_fields):
    if not documents:
        return {
            "attempted": 0,
            "upserted": 0,
            "modified": 0,
            "matched": 0,
        }

    operations = []

    for document in documents:
        filter_query = {
            field_name: document[field_name]
            for field_name in unique_fields
        }

        operations.append(
            UpdateOne(
                filter_query,
                {"$set": document},
                upsert=True,
            )
        )

    result = collection.bulk_write(operations, ordered=False)

    return {
        "attempted": len(documents),
        "upserted": result.upserted_count,
        "modified": result.modified_count,
        "matched": result.matched_count,
    }


def create_aggregate_indexes(db):
    db["page_activity_by_minute"].create_index(
        [("page_key", 1), ("event_minute", -1)],
        unique=True,
    )

    db["user_activity_by_minute"].create_index(
        [("user", 1), ("event_minute", -1)],
        unique=True,
    )

    db["bot_human_activity_by_minute"].create_index(
        [("event_minute", -1)],
        unique=True,
    )


def aggregate_page_activity(db):
    processed_events = db["processed_events"]
    page_activity = db["page_activity_by_minute"]

    results = list(processed_events.aggregate([
        {"$match": {"event_type": {"$in": ["edit", "new"]}}},
        {
            "$group": {
                "_id": {
                    "page_key": "$page_key",
                    "event_minute": "$event_minute",
                },
                "edit_count": {"$sum": 1},
                "human_edit_count": {
                    "$sum": {"$cond": [{"$eq": ["$is_bot", False]}, 1, 0]}
                },
                "bot_edit_count": {
                    "$sum": {"$cond": [{"$eq": ["$is_bot", True]}, 1, 0]}
                },
                "unknown_actor_count": {
                    "$sum": {"$cond": [{"$eq": ["$is_bot", None]}, 1, 0]}
                },
                "title": {"$first": "$title"},
                "wiki": {"$first": "$wiki"},
                "language_code": {"$first": "$language_code"},
                "language_name": {"$first": "$language_name"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "page_key": "$_id.page_key",
                "event_minute": "$_id.event_minute",
                "title": 1,
                "wiki": 1,
                "language_code": 1,
                "language_name": 1,
                "edit_count": 1,
                "human_edit_count": 1,
                "bot_edit_count": 1,
                "unknown_actor_count": 1,
            }
        },
    ]))

    return upsert_aggregate_documents(
        page_activity,
        results,
        ["page_key", "event_minute"],
    )


def aggregate_user_activity(db):
    processed_events = db["processed_events"]
    user_activity = db["user_activity_by_minute"]

    results = list(processed_events.aggregate([
        {
            "$match": {
                "event_type": {"$in": ["edit", "new"]},
                "user": {"$ne": None},
            }
        },
        {
            "$group": {
                "_id": {
                    "user": "$user",
                    "event_minute": "$event_minute",
                },
                "edit_count": {"$sum": 1},
                "human_edit_count": {
                    "$sum": {"$cond": [{"$eq": ["$is_bot", False]}, 1, 0]}
                },
                "bot_edit_count": {
                    "$sum": {"$cond": [{"$eq": ["$is_bot", True]}, 1, 0]}
                },
                "wiki": {"$first": "$wiki"},
                "language_code": {"$first": "$language_code"},
                "language_name": {"$first": "$language_name"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "user": "$_id.user",
                "event_minute": "$_id.event_minute",
                "wiki": 1,
                "language_code": 1,
                "language_name": 1,
                "edit_count": 1,
                "human_edit_count": 1,
                "bot_edit_count": 1,
            }
        },
    ]))

    return upsert_aggregate_documents(
        user_activity,
        results,
        ["user", "event_minute"],
    )


def aggregate_bot_human_activity(db):
    processed_events = db["processed_events"]
    bot_human_activity = db["bot_human_activity_by_minute"]

    results = list(processed_events.aggregate([
        {"$match": {"event_type": {"$in": ["edit", "new"]}}},
        {
            "$group": {
                "_id": "$event_minute",
                "total_count": {"$sum": 1},
                "human_count": {
                    "$sum": {"$cond": [{"$eq": ["$is_bot", False]}, 1, 0]}
                },
                "bot_count": {
                    "$sum": {"$cond": [{"$eq": ["$is_bot", True]}, 1, 0]}
                },
                "unknown_count": {
                    "$sum": {"$cond": [{"$eq": ["$is_bot", None]}, 1, 0]}
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "event_minute": "$_id",
                "total_count": 1,
                "human_count": 1,
                "bot_count": 1,
                "unknown_count": 1,
                "human_ratio": {
                    "$cond": [
                        {"$eq": ["$total_count", 0]},
                        0,
                        {"$divide": ["$human_count", "$total_count"]},
                    ]
                },
                "bot_ratio": {
                    "$cond": [
                        {"$eq": ["$total_count", 0]},
                        0,
                        {"$divide": ["$bot_count", "$total_count"]},
                    ]
                },
            }
        },
    ]))

    return upsert_aggregate_documents(
        bot_human_activity,
        results,
        ["event_minute"],
    )


def run_aggregations():
    validate_config()
    db = get_database()

    create_aggregate_indexes(db)

    page_summary = aggregate_page_activity(db)
    user_summary = aggregate_user_activity(db)
    bot_human_summary = aggregate_bot_human_activity(db)

    print("[INFO] Page activity:", page_summary)
    print("[INFO] User activity:", user_summary)
    print("[INFO] Bot/human activity:", bot_human_summary)


if __name__ == "__main__":
    run_aggregations()