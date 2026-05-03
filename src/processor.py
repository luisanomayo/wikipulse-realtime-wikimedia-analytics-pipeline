
from datetime import datetime, timezone

import wikipedia


WIKIPEDIA_LANGUAGES = wikipedia.languages()

KNOWN_SCHEMAS = {
    "/mediawiki/recentchange/1.0.0"
    }

REQUIRED_TOP_FIELDS = {
    "type",
    "wiki",
    "title"
}

#data cleaning functions
#clean text fields by stripping leading/trailing whitespace
#normalize boolean fields to True/False

def clean_text(value):
    if isinstance(value, str):
        return value.strip()
    return value

def normalize_boolean(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        value_lower = value.lower()
        if value_lower in ['true', '1', 'yes']:
            return True
        elif value_lower in ['false', '0', 'no']:
            return False
    return None

#check if event has a known schema and required fields
def get_schema_status(event: dict) -> str:
    """
    Validates a recentchange event against the known schema and required fields.
    
    Known schema -> goes to processing
    Unknown schema + relevant fileds -> goes to processing with warning
    Unknown schema + missing relevant fields -> goes to rejected events
    Missing schema -> goes to rejected events collection for further analysis
    

    Args:
        event (dict): The recent change event to validate.    
    """
    
    schema_url = event.get("$schema")
    
    if not schema_url:
        return "missing_schema"
    
    if schema_url in KNOWN_SCHEMAS:
        return "known_schema"
    
    return "unknown_schema"


#check if event for wikipedia
def is_wikipedia_project(server_name: str | None) -> bool:
    if not isinstance(server_name, str):
        return False
    return server_name.strip().lower().endswith("wikipedia.org")

#extract language code for analytics filtering if needed
def extract_language_code(server_name: str | None) -> str | None:
    if not is_wikipedia_project(server_name):
        return None
    return server_name.strip().split(".")[0]

#get language name from code using wikipedia library
def get_language_name(language_code: str) -> str | None:
    if not language_code:
        return None

    return WIKIPEDIA_LANGUAGES.get(language_code)

#check for relevant columns and output list of missing fields
def check_required_fields(event: dict) -> list[str]:
    
    """
    Checks if a recent change event contains all required top-level fields.

    Args:
        event (dict): The recent change event to check.    
    """
    missing_fields = []
    
    meta = event.get("meta", {})
    
    if not meta.get('id'):
        missing_fields.append("meta.id")
        
    #check for timestamp field or meta.timestamp field
    timestamp = event.get("timestamp") or meta.get("dt")
    if not timestamp:
        missing_fields.append("timestamp or meta.dt")
    
    for field in REQUIRED_TOP_FIELDS:
        if not event.get(field):
            missing_fields.append(field)
            
    return missing_fields

#time decomposition function
def decompose_timestamp(events: dict) -> dict | None:
    """
    Decomposes a UNIX timestamp or timestamptz meta datetime field

    Args:
        event (dict): The recent change event containing the timestamp to decompose.

    Returns:
        dict: A dictionary containing the decomposed time components.
    """
    meta = events.get("meta", {})
    unix_timestamp = events.get("timestamp")
    meta_datetime = meta.get("dt")
    
    if unix_timestamp :
        event_datetime = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
    elif meta_datetime:
        cleaned_meta_datetime = meta_datetime.replace("Z", "+00:00")
        event_datetime = datetime.fromisoformat(cleaned_meta_datetime)
    else:
        return None
    
    return {
        "event_timestamp": event_datetime.isoformat(),
        "event_date": event_datetime.date().isoformat(),
        "event_hour": event_datetime.replace(minute=0, second=0, microsecond=0).isoformat(),
        "event_minute": event_datetime.replace(second=0, microsecond=0).isoformat()
    }
    
def process_recentchange_event(event: dict) -> dict:
    """
    process recentchange events by filtering for wikipedia events,validating against known schema 
    and required fields, decomposing timestamp, 
    and returning a cleaned event dict ready for analysis or storage.
    """
    server_name = clean_text(event.get("server_name"))
    
    if not is_wikipedia_project(server_name):
        return {
            "status": "skipped",
            "reason": "not a wikipedia project"
        }
    
    schema_status = get_schema_status(event)
    missing_fields = check_required_fields(event)
    
    if missing_fields:
        return {
            "status": "rejected",
            "reason": f"Missing required fields: {', '.join(missing_fields)}",
            "raw_event": event
        }
        
    time_fields = decompose_timestamp(event)
    
    if time_fields is None:
        return {
            "status": "rejected",
            "reason": "invalid or missing timestamp",
            "raw_event": event
        }
        
    meta = event.get("meta", {})
    
    event_type = clean_text(event.get("type"))
    wiki = clean_text(event.get("wiki"))
    title = clean_text(event.get("title"))
    user = clean_text(event.get("user"))
    comment = clean_text(event.get("comment"))
    is_bot = normalize_boolean(event.get("bot"))
    is_minor = normalize_boolean(event.get("minor"))
    
    language_code = extract_language_code(server_name)
    language_name = get_language_name(language_code)
    
    #create unique text identifier for each page using wiki + title
    page_key = f"{wiki}:{title}"
    
    processed_event = {
        "status": "processed",
        "event_id": meta.get("id"),
        "schema_status": schema_status,
        "event_type": event_type,
        "wiki": wiki,
        #server name as event.get('server_name') or meta.get('domain') to handle cases where server_name is missing but domain is present in meta
        "server_name": clean_text(event.get("server_name") or meta.get("domain")),
        "server_url": clean_text(event.get("server_url") or meta.get("uri")),
        "language_code": language_code,
        "language_name": language_name,
        "title": title,
        "page_key": page_key,
        "namespace": event.get("namespace"),
        "user": user,
        "is_bot": is_bot,
        "is_minor": is_minor,
        "comment": comment,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        **time_fields
    }
    
    #add log fields only if present in the event
    if event_type == "log":
        processed_event["log_action"] = clean_text(event.get("log_action"))
        processed_event["log_type"] = clean_text(event.get("log_type"))
        
    return processed_event
