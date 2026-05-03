# Technical Design: WikiPulse Real-Time Wikimedia Streaming Pipeline

## 1. Business Context

WikiPulse is designed for a digital intelligence use case where organizations want to detect emerging global events earlier than they might through traditional news monitoring.

The core assumption is that Wikipedia article edits often increase around events that people are actively documenting or updating. Examples include breaking news, elections, sports results, cultural moments, and geopolitical developments.

The system addresses:

- continuous real-time event ingestion
- noise reduction, especially bot activity
- scalable ingestion architecture
- low-latency analytics
- visibility into emerging topics

---

## 2. End-to-End Data Flow

```text
Wikimedia EventStreams
        ↓
Python ingestion service
        ↓
Google Pub/Sub topic
        ↓
Google Pub/Sub subscription
        ↓
Python subscriber processor
        ↓
MongoDB event-level collections
        ↓
MongoDB aggregate collections
        ↓
Analytics queries
```

### Step 1: Ingestion

`ingest_stream.py` connects to the Wikimedia recent change stream using Server-Sent Events.

The ingestion layer performs lightweight validation:

- ignore non-message SSE events
- confirm the payload is valid JSON
- remove canary events
- require `meta.id`

The ingestion layer then publishes the raw valid event to Pub/Sub. This keeps ingestion thin and avoids mixing source collection with business processing.

### Step 2: Pub/Sub Messaging

Google Pub/Sub acts as the decoupling layer between ingestion and downstream processing.

The ingestion script is the publisher. It sends raw Wikimedia events to a Pub/Sub topic. The subscriber processor reads from a Pub/Sub subscription. This means ingestion does not need to know how downstream processing, storage, or analytics are implemented.

### Step 3: Stream Processing

`subscriber_processor.py` consumes messages from Pub/Sub and sends each event to `processor.py`.

The processor:

- filters to Wikipedia projects
- validates required fields
- checks schema status
- derives language information
- standardizes fields
- performs light text cleaning
- creates derived time fields
- separates processed, rejected, and skipped events

Only Wikipedia events are stored. Non-Wikipedia events are skipped and not persisted.

### Step 4: Storage

`mongo_storage.py` handles MongoDB connection, indexes, and inserts.

The storage layer writes to:

- `raw_events`
- `processed_events`
- `rejected_events`

### Step 5: Analytics Aggregation

`analytics_aggregates.py` reads from `processed_events` and creates minute-level aggregate collections:

- `page_activity_by_minute`
- `user_activity_by_minute`
- `bot_human_activity_by_minute`

These collections support low-latency dashboard-style queries.

---

## 3. Source Data

The source is Wikimedia EventStreams, specifically the recent change stream.

Each event may contain fields such as:

- `$schema`
- `meta`
- `id`
- `type`
- `title`
- `timestamp`
- `user`
- `bot`
- `minor`
- `server_name`
- `wiki`
- `comment`

The stream includes events from many Wikimedia projects, not only Wikipedia. Since the project focuses on Wikipedia article edit activity, the processing layer filters to events where:

```text
server_name ends with ".wikipedia.org"
```

---

## 4. Design Decisions

### 4.1 Pub/Sub as a Decoupling Layer

Pub/Sub separates ingestion from processing and storage.

Without Pub/Sub, the pipeline would be tightly coupled:

```text
ingestion → processing → storage
```

If MongoDB or processing failed, ingestion would also be affected.

With Pub/Sub:

```text
ingestion → Pub/Sub → processing/storage
```

The ingestion service only publishes events. Downstream services can fail, recover, or scale independently.

### 4.2 Publish Raw Events, Not Half-Processed Events

The ingestion layer publishes raw events after minimal validation.

This avoids:

- larger message payloads
- duplicated raw and processed data in Pub/Sub
- unclear ownership of transformation logic
- unnecessary latency during ingestion

Processing responsibility belongs downstream.

### 4.3 Remove Canary Events

Wikimedia streams may include canary events used for monitoring. These are not real edit events and are removed during ingestion because they are not analytically meaningful.

### 4.4 Use `meta.id` as the Event Identifier

Initial profiling showed that the top-level `id` field was not always available, while `meta.id` was consistently available and unique in the sampled data.

Therefore, `meta.id` is used as:

- `event_id`
- MongoDB unique key
- deduplication field

### 4.5 Timestamp Fallback: `timestamp` or `meta.dt`

Events include both:

- `timestamp`: Unix timestamp
- `meta.dt`: ISO datetime string

These represent the same event time in the sampled data. The processor uses `timestamp` first and falls back to `meta.dt` if needed. This avoids rejecting otherwise usable events if one timestamp representation is missing.

### 4.6 Schema Status Instead of Hard Schema Rejection

The processor checks whether `$schema` is present and whether it belongs to the known schema set.

Schema status is recorded as:

- `known_schema`
- `unknown_schema`
- `missing_schema`

Unknown schema does not automatically cause rejection. The event is rejected only if required fields are missing. This separates schema governance from event usability.

### 4.7 Wikipedia Project Filtering

The source stream includes many Wikimedia projects, but the use case focuses on Wikipedia as the most viable source for detecting public knowledge edits and emerging topics.

The system filters to events where:

```text
server_name ends with ".wikipedia.org"
```

This keeps language-specific Wikipedia projects and excludes projects such as Commons, Wikidata, MediaWiki, and Wikispecies.

This filtering happens in the processing layer, not ingestion, so ingestion remains source-agnostic.

### 4.8 Language Handling

Language is derived from the Wikipedia subdomain:

```text
en.wikipedia.org → en
simple.wikipedia.org → simple
fr.wikipedia.org → fr
```

The system keeps:

- `language_code`
- `language_name`

The project considered country/language libraries such as `pycountry`, but Wikimedia language codes are not always standard ISO codes. The `wikipedia` Python package was used because it provides Wikipedia language code mappings.

The language name may be in native script rather than English. The pipeline treats `language_code` as the analytical key and language name as display metadata.

### 4.9 Event-Based Data Modelling

The system stores one event per document instead of one page document containing many edits.

Event-based modelling is better because:

- the source data is event-based
- inserts are append-only
- time-window queries are easier
- deduplication is simpler
- documents do not grow without limit
- aggregation can happen flexibly by page, user, time, language, or bot status

A page-based model would require repeated updates to the same document, risk unbounded document growth, and complicate concurrent writes.

---

## 5. Processing Design

### 5.1 Required Fields

The event must have:

- `meta.id`
- `type`
- `wiki`
- `title`
- `timestamp` or `meta.dt`

Fields such as `user`, `bot`, and `minor` are analytically useful but not mandatory. Events without these fields can still support page-level or time-based analytics, even if they cannot support user-specific or bot-specific views.

### 5.2 Cleaning

The processor performs lightweight cleaning:

- strip whitespace from text fields
- normalize boolean fields to `True`, `False`, or `None`
- standardize timestamp fields
- flatten nested metadata
- derive event-level fields

The system avoids heavy text processing because the current use case is based on edit activity patterns, not article content NLP.

### 5.3 Derived Fields

The processor creates:

- `event_timestamp`
- `event_date`
- `event_hour`
- `event_minute`
- `page_key`
- `language_code`
- `language_name`

`page_key` combines `wiki` and `title`:

```text
wiki:title
```

This prevents collisions between pages with the same title in different Wikipedia projects.

### 5.4 Processed, Rejected, and Skipped Events

The processor can return three statuses:

#### `processed`

The event is a valid Wikipedia event and is stored.

#### `rejected`

The event is from Wikipedia but is missing required fields or cannot be processed. Rejected events are stored in `rejected_events`.

#### `skipped`

The event is valid but outside the business scope, such as non-Wikipedia Wikimedia projects. Skipped events are not stored to optimize storage usage.

---

## 6. MongoDB Storage Design

### 6.1 Collections

#### `raw_events`

Stores original Wikipedia event payloads wrapped with metadata:

```json
{
  "event_id": "...",
  "ingested_at": "...",
  "raw_event": {}
}
```

Purpose:

- audit trail
- replay
- debugging

#### `processed_events`

Stores cleaned and standardized event documents.

Purpose:

- source of truth for analytics

#### `rejected_events`

Stores invalid Wikipedia events with rejection reason and missing fields.

Purpose:

- data quality monitoring
- debugging rejected records

### 6.2 Raw Event Retention Choice

Raw events are stored only for Wikipedia events. This supports replay and auditability while avoiding unbounded storage of events outside the business scope.

### 6.3 Idempotency

Idempotency is handled with a unique index on `event_id`.

If Pub/Sub redelivers a message or the pipeline receives a duplicate event, MongoDB prevents duplicate inserts.

For event-level storage, the system uses insert-only logic because events are append-only and not expected to change after arrival.

### 6.4 Insert Strategy

The notebook prototype used batch inserts. The subscriber script currently performs single-message processing because Pub/Sub callbacks handle messages individually.

The design can be extended to batch writes for higher throughput.

---

## 7. Indexing Strategy

### 7.1 `raw_events`

Indexes:

- `event_id` unique
- `ingested_at`

Purpose:

- deduplication
- replay/debugging by ingestion time

### 7.2 `processed_events`

Indexes:

- `event_id` unique
- `event_timestamp`
- `(page_key, event_timestamp)`
- `(user, event_timestamp)`
- `(is_bot, event_timestamp)`
- `(event_type, event_timestamp)`
- `(wiki, event_timestamp)`
- `(language_code, event_timestamp)`
- `(server_name, event_timestamp)`

Purpose:

- time-window analytics
- top edited pages
- user activity queries
- bot/human analysis
- language-specific filtering
- idempotency

### 7.3 `rejected_events`

Indexes:

- `reason`
- `ingested_at`

Purpose:

- debugging validation failures
- monitoring rejection patterns

### 7.4 Aggregate Collections

#### `page_activity_by_minute`

Unique index:

```text
(page_key, event_minute)
```

#### `user_activity_by_minute`

Unique index:

```text
(user, event_minute)
```

#### `bot_human_activity_by_minute`

Unique index:

```text
event_minute
```

These indexes ensure one aggregate document per entity per time bucket and support upsert correctness.

---

## 8. Analytics Aggregation Layer

The analytics layer creates minute-level aggregate collections from `processed_events`.

### 8.1 Why Aggregate Collections Are Used

The system could query `processed_events` directly, but that becomes expensive as data volume grows.

Aggregate collections are used because they:

- reduce query cost
- support low-latency dashboards
- avoid repeatedly scanning event-level data
- create analytics-ready summaries

This is standard in streaming analytics systems: event-level data is the source of truth, while aggregate data supports fast serving queries.

### 8.2 Aggregate Collections

#### `page_activity_by_minute`

Supports top edited pages in a time window.

Fields include:

- `page_key`
- `title`
- `wiki`
- `language_code`
- `event_minute`
- `edit_count`
- `human_edit_count`
- `bot_edit_count`
- `unknown_actor_count`

#### `user_activity_by_minute`

Supports edit activity per user.

Fields include:

- `user`
- `event_minute`
- `edit_count`
- `human_edit_count`
- `bot_edit_count`

#### `bot_human_activity_by_minute`

Supports bot vs human activity ratio.

Fields include:

- `event_minute`
- `total_count`
- `human_count`
- `bot_count`
- `unknown_count`
- `human_ratio`
- `bot_ratio`

### 8.3 Aggregation Upserts

Aggregate collections use upserts because the same time bucket may receive new events over time.

This differs from event-level storage:

```text
event-level data = insert only
aggregate data = update or insert
```

---

## 9. Near Real-Time Design

The intended runtime setup is:

```text
Terminal 1: python src/ingest_stream.py
Terminal 2: python src/subscriber_processor.py
Terminal 3: python src/analytics_aggregates.py
```

`ingest_stream.py` and `subscriber_processor.py` are continuous processes.

`analytics_aggregates.py` can be run periodically, for example every minute, to refresh aggregate collections.

This means:

- ingestion is continuous
- processing and storage are continuous
- analytics are near real-time based on aggregation frequency

A more advanced version could update aggregate collections directly inside the subscriber processor, but the current design separates event storage from analytics aggregation for clarity and maintainability.

---

## 10. Trade-Offs

### 10.1 Filtering Before Storage

The project filters non-Wikipedia events before storage.

Pros:

- reduces storage usage
- keeps the database aligned to the business case
- avoids storing irrelevant Wikimedia project activity

Cons:

- non-Wikipedia data cannot be replayed later

This was accepted because the project scope specifically focuses on Wikipedia edit intelligence.

### 10.2 Keeping All Wikipedia Languages

The project keeps all Wikipedia language editions.

Pros:

- supports global intelligence
- allows later language-specific filtering

Cons:

- increases data volume compared to English-only filtering

This was accepted because the business case is global.

### 10.3 Aggregate Script Instead of Fully Streaming Aggregates

The aggregation layer currently runs as a separate script.

Pros:

- simpler
- easier to test
- easier to explain

Cons:

- not fully real-time at the aggregate level

This creates a near real-time design where aggregation frequency determines freshness.

### 10.4 MongoDB Instead of Relational Tables

MongoDB was chosen because Wikimedia events are semi-structured and event types may not share all fields.

Pros:

- flexible schema
- good fit for event documents
- easy to store raw and processed JSON-like data

Cons:

- analytics queries require careful indexing and aggregation design

### 10.5 Not Requiring `user`, `bot`, or `minor`

These fields are analytically useful but not required.

Pros:

- preserves more valid page/time events

Cons:

- some records may be excluded from user or bot-specific analytics

Missing values are treated as unknown where relevant.

---

## 11. Challenges Encountered

### 11.1 Wikimedia User-Agent Requirement

Initial requests returned a 403 error until a proper User-Agent header was added.

Resolution:

- User-Agent was moved into `.env`
- the stream connection includes the header

### 11.2 JSONL Writing Issue

During notebook prototyping, writing event records and newline characters together caused issues.

Resolution:

- JSON write and newline write were separated

### 11.3 MongoDB Atlas TLS Certificate Issue

MongoDB connection initially failed due to SSL certificate verification.

Resolution:

- `certifi` was installed
- MongoClient was configured with `tlsCAFile=certifi.where()`

### 11.4 Google Credentials Loading

Google Pub/Sub initially failed with `DefaultCredentialsError`.

Resolution:

- used the correct environment variable: `GOOGLE_APPLICATION_CREDENTIALS`
- reran `load_dotenv()` after adding GCP variables

### 11.5 Pub/Sub Publisher Blocking

The ingestion script initially appeared to publish zero events because the publisher helper was not structured correctly for repeated publishing.

Resolution:

- publisher client is created once
- `future.result(timeout=30)` is used
- publishing was tested independently before full ingestion

### 11.6 Storage Boundary Confusion

Storage logic initially lived inside the subscriber script.

Resolution:

- MongoDB-specific logic was moved into `mongo_storage.py`
- subscriber now orchestrates message handling while storage functions manage database writes

---

## 12. Scalability Approach

### 12.1 Pub/Sub Decoupling

Pub/Sub separates ingestion from downstream processing.

This allows:

- independent scaling of subscribers
- buffering during spikes
- retry/redelivery if processing fails
- adding more downstream consumers later

### 12.2 Event-Based Storage

Event-level documents support append-only writes, which are simpler to scale than constantly updating large page documents.

### 12.3 Unique Event IDs

Unique indexes on `event_id` prevent duplicate processing during retries or redelivery.

### 12.4 Aggregate Collections

Aggregate collections reduce the need to repeatedly scan detailed events for dashboards.

This supports lower-latency analytics as data volume grows.

### 12.5 Indexing

Indexes were created around expected query patterns:

- time-window queries
- page activity
- user activity
- bot/human distribution
- language filtering

### 12.6 Future Improvements

Possible scalability improvements include:

- batching MongoDB writes in the subscriber
- running multiple subscriber instances
- using dead-letter topics for repeated failures
- scheduling aggregation with Cloud Scheduler or cron
- moving aggregation into a streaming processor
- adding TTL indexes for raw event retention
- deploying components as containerized services

---

## 13. Final Architecture Summary

The final architecture is:

```text
Wikimedia EventStreams
        ↓
Python ingestion service
        ↓
Google Pub/Sub topic
        ↓
Pub/Sub subscription
        ↓
Python subscriber processor
        ↓
MongoDB event-level collections
        ↓
MongoDB aggregate collections
        ↓
Near real-time analytics
```

This design balances clarity, scalability, storage control, and business relevance.
