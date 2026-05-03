import json
from google.cloud import pubsub_v1

from config import (
    GCP_PROJECT_ID,
    PUBSUB_SUBSCRIPTION_ID,
    validate_config,
)

from mongo_storage import (
    get_mongo_collections,
    create_storage_indexes,
    store_processed_event,
    store_rejected_event,
)

try:
    from processor import process_recentchange_event
except Exception as error:
    print(f"[ERROR] Failed to load processor: {error}")
    print(
        "[ERROR] Processor startup depends on wikipedia.languages(). "
        "Check that the wikipedia package is installed in the active venv "
        "and that Wikipedia is reachable when starting the subscriber."
    )
    raise


def handle_message(message, collections):
    raw_event = json.loads(message.data.decode("utf-8"))

    result = process_recentchange_event(raw_event)

    if result["status"] == "skipped":
        message.ack()
        return

    if result["status"] == "processed":
        store_processed_event(collections, raw_event, result)
        message.ack()
        print(f"[INFO] Processed event: {result['event_id']}")
        return

    if result["status"] == "rejected":
        store_rejected_event(collections, result)
        message.ack()
        print("[INFO] Rejected event stored")
        return


def run_subscriber():
    validate_config()

    collections = get_mongo_collections()
    create_storage_indexes(collections)

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        GCP_PROJECT_ID,
        PUBSUB_SUBSCRIPTION_ID,
    )

    print(f"[INFO] Listening to: {subscription_path}")

    def callback(message):
        try:
            handle_message(message, collections)
        except Exception as error:
            print(f"[ERROR] Failed to process message: {error}")
            message.nack()

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback,
    )

    with subscriber:
        streaming_pull_future.result()


if __name__ == "__main__":
    run_subscriber()
