import os, time, requests, psycopg2, json, datetime
from psycopg2.extras import execute_values

VERSION = "1.0.4"
ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN")
ZENDESK_EMAIL = os.getenv("ZENDESK_EMAIL")
ZENDESK_TOKEN = os.getenv("ZENDESK_TOKEN")
PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")
MAX_REQUESTS_PER_MIN = int(os.getenv("MAX_REQUESTS_PER_MIN", 400))
INITIAL_FULL_PULL = os.getenv("INITIAL_FULL_PULL", "true").lower() == "true"
INCREMENTAL_INTERVAL_SECONDS = int(os.getenv("INCREMENTAL_INTERVAL_SECONDS", 600))

conn = psycopg2.connect(
    host=PGHOST, port=PGPORT, dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD
)
cur = conn.cursor()

def fetch_tickets(url):
    res = requests.get(url, auth=(f"{ZENDESK_EMAIL}/token", ZENDESK_TOKEN))
    res.raise_for_status()
    data = res.json()
    # Debug logging
    print(f"📊 API Response - Tickets in batch: {len(data.get('tickets', []))}")
    print(f"📊 Has 'next' link: {bool(data.get('links', {}).get('next'))}")
    if 'meta' in data:
        print(f"📊 Meta info: {data['meta']}")
    return data

def upsert_tickets(tickets):
    rows = []
    for t in tickets:
        # Convert dict/list to JSON string only for JSONB columns
        def to_json(value):
            if value is None:
                return None
            if isinstance(value, (dict, list)):
                return json.dumps(value)
            return value

        rows.append((
            t.get("id"), t.get("url"), t.get("external_id"),
            t.get("created_at"), t.get("updated_at"), t.get("type"),
            t.get("subject"), t.get("raw_subject"), t.get("description"),
            t.get("priority"), t.get("status"), t.get("recipient"),
            t.get("requester_id"), t.get("submitter_id"), t.get("assignee_id"),
            t.get("organization_id"), t.get("group_id"), t.get("brand_id"),
            t.get("ticket_form_id"), t.get("problem_id"), t.get("has_incidents"),
            t.get("due_at"),
            # Array columns - pass as Python lists
            t.get("collaborator_ids"), t.get("follower_ids"),
            t.get("email_cc_ids"), t.get("sharing_agreement_ids"),
            t.get("tags"),
            # JSONB columns - convert to JSON strings
            to_json(t.get("via")), to_json(t.get("satisfaction_rating")),
            to_json(t.get("custom_fields")), to_json(t)
        ))

    execute_values(cur, """
        INSERT INTO zendesk_tickets (
            id, url, external_id, created_at, updated_at,
            type, subject, raw_subject, description, priority,
            status, recipient, requester_id, submitter_id, assignee_id,
            organization_id, group_id, brand_id, ticket_form_id,
            problem_id, has_incidents, due_at,
            collaborator_ids, follower_ids, email_cc_ids, sharing_agreement_ids,
            tags, via, satisfaction_rating, custom_fields, raw
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            updated_at = EXCLUDED.updated_at,
            status = EXCLUDED.status,
            assignee_id = EXCLUDED.assignee_id,
            custom_fields = EXCLUDED.custom_fields,
            raw = EXCLUDED.raw;
    """, rows, page_size=100)
    conn.commit()

def run_sync():
    print(f"🔁 Starting Zendesk sync... (v{VERSION})")
    
    # Use Incremental Ticket Export API to get ALL tickets (no limits)
    # This API is specifically designed for full exports and syncs
    if INITIAL_FULL_PULL:
        # Start from Unix epoch (0) to get all tickets ever created
        start_time = 0
        print("📦 Running FULL SYNC - fetching all tickets from the beginning...")
    else:
        # Get the most recent ticket's updated_at timestamp from DB for incremental sync
        cur.execute("SELECT MAX(updated_at) FROM zendesk_tickets")
        result = cur.fetchone()[0]
        if result:
            start_time = int(datetime.datetime.fromisoformat(result.replace('Z', '+00:00')).timestamp())
            print(f"🔄 Running INCREMENTAL SYNC from timestamp {start_time}...")
        else:
            start_time = 0
            print("📦 No existing tickets found - running FULL SYNC...")
    
    url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/incremental/tickets.json?start_time={start_time}"
    count = 0
    page_num = 0
    
    while url:
        page_num += 1
        print(f"📄 Fetching page {page_num}...")
        data = fetch_tickets(url)
        tickets = data.get("tickets", [])
        
        if not tickets:
            print("⚠️  No tickets in response")
            # Check if we've reached the end
            if data.get("end_of_stream", False):
                print("✅ Reached end of stream")
                break
            # If end_time is present, continue from there
            if "end_time" in data:
                url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/incremental/tickets.json?start_time={data['end_time']}"
                continue
            break
        
        upsert_tickets(tickets)
        count += len(tickets)
        print(f"✅ Upserted {count} tickets so far...")
        
        # Check if we've reached the end of the stream
        if data.get("end_of_stream", False):
            print("✅ Reached end of stream")
            break
        
        # Get next URL or construct from end_time
        next_url = data.get("next_page")
        if next_url:
            url = next_url
        elif "end_time" in data:
            url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/incremental/tickets.json?start_time={data['end_time']}"
        else:
            print("⚠️  No next_page or end_time in response")
            break
        
        time.sleep(60 / MAX_REQUESTS_PER_MIN)
    
    print(f"🎯 Sync complete — {count} tickets total.")

if __name__ == "__main__":
    while True:
        run_sync()
        if INCREMENTAL_INTERVAL_SECONDS == 0:
            break
        print(f"Sleeping {INCREMENTAL_INTERVAL_SECONDS}s before next incremental sync...")
        time.sleep(INCREMENTAL_INTERVAL_SECONDS)

