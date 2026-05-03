import json
from requests_sse import EventSource

from config import WIKIMEDIA_STREAM_URL, WIKIMEDIA_USER_AGENT, validate_config


validate_config()

headers = {
    "User-Agent": WIKIMEDIA_USER_AGENT
}

print(f"URL: {WIKIMEDIA_STREAM_URL}")
print(f"User-Agent exists: {bool(WIKIMEDIA_USER_AGENT)}")
print("Connecting...")

with EventSource(WIKIMEDIA_STREAM_URL, headers=headers, timeout=30) as event_source:
    for stream_event in event_source:
        print(f"Event type: {stream_event.type}")

        if stream_event.type != "message":
            continue

        event_data = json.loads(stream_event.data)
        print(event_data.keys())
        print(event_data.get("title"))
        break

print("Done")