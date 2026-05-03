import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# MongoDB connection variables
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DB")

# GCP variables
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PUBSUB_TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID")
PUBSUB_SUBSCRIPTION_ID = os.getenv("PUBSUB_SUBSCRIPTION_ID")

#WIKIMEDIA STREAM VARIABLES
WIKIMEDIA_STREAM_URL = os.getenv("WIKIMEDIA_STREAM_URL")
WIKIMEDIA_USER_AGENT = os.getenv("WIKIMEDIA_USER_AGENT")


def validate_config():
    required_variables = {
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "GOOGLE_APPLICATION_CREDENTIALS": GOOGLE_APPLICATION_CREDENTIALS,
        "PUBSUB_TOPIC_ID": PUBSUB_TOPIC_ID,
        "PUBSUB_SUBSCRIPTION_ID": PUBSUB_SUBSCRIPTION_ID,
        "WIKIMEDIA_STREAM_URL": WIKIMEDIA_STREAM_URL,
        "WIKIMEDIA_USER_AGENT": WIKIMEDIA_USER_AGENT,
        "MONGODB_URI": MONGODB_URI,
        "MONGODB_DATABASE": MONGODB_DATABASE,
    }

    missing_variables = [
        variable_name
        for variable_name, variable_value in required_variables.items()
        if not variable_value
    ]

    if missing_variables:
        raise ValueError(f"Missing environment variables: {missing_variables}")

    credentials_path = Path(GOOGLE_APPLICATION_CREDENTIALS)

    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Google credentials file not found: {GOOGLE_APPLICATION_CREDENTIALS}"
        )

    return True


if __name__ == "__main__":
    validate_config()
    print("Config validation successful")
