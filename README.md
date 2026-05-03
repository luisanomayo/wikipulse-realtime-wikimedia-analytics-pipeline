# WikiPulse: Real-Time Wikimedia Streaming Pipeline

WikiPulse is a real-time data streaming pipeline that ingests live Wikimedia EventStreams data, publishes valid events to Google Pub/Sub, processes Wikipedia edit activity, stores event-level records in MongoDB, and creates analytics-ready aggregates for near real-time intelligence.

The project is based on the idea that Wikipedia edits can act as early signals for breaking news, political developments, sports outcomes, cultural trends, and other global knowledge events.

---

## Business Problem

A digital intelligence company wants to detect and understand global events as they unfold in real time. Traditional news sources can be delayed, while Wikipedia edit activity often reflects emerging public attention around important topics.

The system addresses:

- continuous real-time event ingestion
- noise reduction, especially bot activity
- scalable high-throughput ingestion
- low-latency analytics on evolving data
- visibility into trending topics as they emerge

---

## Architecture Overview

```text
Wikimedia EventStreams
        ↓
ingest_stream.py
        ↓
Google Pub/Sub Topic
        ↓
subscriber_processor.py
        ↓
processor.py
        ↓
mongo_storage.py
        ↓
MongoDB Event Collections
        ↓
analytics_aggregates.py
        ↓
MongoDB Aggregate Collections
```

---

## Data Flow

1. `ingest_stream.py` connects to Wikimedia EventStreams and listens for recent change events.
2. Valid raw events are published to a Google Pub/Sub topic.
3. `subscriber_processor.py` consumes raw events from a Pub/Sub subscription.
4. `processor.py` validates, filters, cleans, and standardizes events.
5. `mongo_storage.py` stores Wikipedia events in MongoDB.
6. `analytics_aggregates.py` creates minute-level aggregate collections for analytics.

---

## Key Design Choices

### Wikipedia-only filtering

The Wikimedia stream includes many projects, but the business case focuses on real-time intelligence from Wikipedia article edits. The pipeline stores only events where:

```text
server_name ends with ".wikipedia.org"
```

This keeps Wikipedia language editions such as `en.wikipedia.org`, `fr.wikipedia.org`, and `simple.wikipedia.org`, while excluding projects such as Commons, Wikidata, MediaWiki, and Wikispecies.

### Language handling

Language is derived from the Wikipedia subdomain:

```text
en.wikipedia.org → en
fr.wikipedia.org → fr
simple.wikipedia.org → simple
```

The pipeline stores both `language_code` and `language_name`. The language code is treated as the main analytical field.

### Event-based schema

Each MongoDB document represents one event, not one page. This supports append-only writes, deduplication, replay, time-window analytics, and flexible aggregation by page, user, bot status, language, and event type.

### Pub/Sub decoupling

Google Pub/Sub separates ingestion from downstream processing. The ingestion service only publishes valid raw events, while subscribers handle processing, storage, and analytics.

---

## MongoDB Collections

### Event-level collections

- `raw_events`: original Wikipedia event payloads for audit and replay
- `processed_events`: cleaned and standardized event records
- `rejected_events`: invalid Wikipedia events with rejection reason

### Aggregate collections

- `page_activity_by_minute`: top edited pages over time
- `user_activity_by_minute`: edit activity per user
- `bot_human_activity_by_minute`: bot vs human activity ratio

---

## Analytics Supported

- Top edited pages in a time window
- Edit activity per user
- Bot vs human activity ratio

Aggregates are computed at minute-level granularity to support near real-time monitoring.

---

## Tech Stack

- Python
- Wikimedia EventStreams
- Google Pub/Sub
- MongoDB Atlas
- PyMongo
- requests-sse
- python-dotenv
- certifi
- wikipedia

---

## Project Structure

```text
src/
├── config.py
├── ingest_stream.py
├── pubsub_publisher.py
├── subscriber_processor.py
├── processor.py
├── mongo_storage.py
└── analytics_aggregates.py

docs/
└── technical_design.md
```

---

## Environment Variables

Create a `.env` file based on `.env.example`.

```env
WIKIMEDIA_STREAM_URL=https://stream.wikimedia.org/v2/stream/recentchange
WIKIMEDIA_USER_AGENT=your-project-name/1.0 (your_email@example.com)

GCP_PROJECT_ID=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account.json
PUBSUB_TOPIC_ID=your-topic-id
PUBSUB_SUBSCRIPTION_ID=your-subscription-id

MONGODB_URI=your-mongodb-atlas-uri
MONGODB_DATABASE=your-database-name
```

Do not commit `.env` or service account credentials.

---

## How to Run

Install dependencies:

```bash
pip install -r requirements.txt
```

Validate configuration:

```bash
python src/config.py
```

Run the ingestion publisher:

```bash
python src/ingest_stream.py
```

Run the subscriber processor in another terminal:

```bash
python src/subscriber_processor.py
```

Run analytics aggregation periodically:

```bash
python src/analytics_aggregates.py
```

---

## Near Real-Time Design

The ingestion and subscriber components are designed to run continuously. The aggregation script can be run periodically, for example every minute, to refresh analytics collections.

This creates a near real-time pipeline where raw events are ingested and processed continuously, while analytics views are updated frequently.

---

## Full Technical Design

See `docs/technical_design.md`.
