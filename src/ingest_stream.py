import json
import time

from requests_sse import EventSource

from config import WIKIMEDIA_STREAM_URL, WIKIMEDIA_USER_AGENT, validate_config
from pubsub_publisher import get_publisher, publish_message


RUN_SECONDS = 60  # Run the stream for 60 seconds for testing

HEADERS = {
    'Accept': 'text/event-stream',
    'User-Agent': WIKIMEDIA_USER_AGENT
}


def is_valid_raw_event(event_data: dict) -> bool:
    if event_data.get("meta", {}).get("domain") == "canary":
        return False

    if not event_data.get("meta", {}).get("id"):
        return False

    return True


def run_ingestion(run_seconds: int = RUN_SECONDS):
    validate_config()

    published_count = 0
    skipped_count = 0
    start_time = time.perf_counter()
    end_time = start_time + run_seconds
    
    publisher, topic_path = get_publisher()

    while time.perf_counter() < end_time:
        try:
            print(f"[INFO] Connecting to stream: {WIKIMEDIA_STREAM_URL}")
            with EventSource(WIKIMEDIA_STREAM_URL, headers=HEADERS, timeout=10) as event_source:
                for stream_event in event_source:
                    if time.perf_counter() >= end_time:
                        break

                    if stream_event.type != "message":
                        skipped_count += 1
                        continue

                    try:
                        event_data = json.loads(stream_event.data)
                    except json.JSONDecodeError:
                        skipped_count += 1
                        continue

                    if not is_valid_raw_event(event_data):
                        skipped_count += 1
                        continue

                    print(f"[INFO] Publishing event: {event_data.get('meta', {}).get('id')}")
                    publish_message(publisher, topic_path, event_data)
                    published_count += 1

        except Exception as error:
            print(f"[WARN] Stream disconnected or errored: {error}")
            print("[INFO] Reconnecting...")
            time.sleep(3)

    elapsed_time = time.perf_counter() - start_time

    print(f"[INFO] Published {published_count} events")
    print(f"[INFO] Skipped {skipped_count} events")
    print(f"[INFO] Runtime: {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    run_ingestion()