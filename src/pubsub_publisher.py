import json
from google.cloud import pubsub_v1
from config import GCP_PROJECT_ID, PUBSUB_TOPIC_ID, validate_config

def get_publisher():
    """
    Initializes and returns a Pub/Sub publisher client.
    """
    validate_config()  # Ensure config is valid before creating publisher
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)
    print(f"Publisher client created for topic: {topic_path}")
    
    return publisher, topic_path


def publish_message(publisher, topic_path, message:dict) -> str:
    message_bytes = json.dumps(message, ensure_ascii=False).encode("utf-8")
    future = publisher.publish(topic_path, message_bytes)
    
    return future.result(timeout=30)


if __name__ == "__main__":
    # Example usage
    test_message = {
        "event_type": "edit",
        "server_name": "en.wikipedia.org",
        "title": "Example Page",
        "timestamp": "2026-05-03T12:00:00Z"
    }
    
    publisher, topic_path = get_publisher()
    message_id = publish_message(publisher, topic_path, test_message)
    print(f"Message published with ID: {message_id}")