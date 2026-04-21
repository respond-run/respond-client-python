# respond-client-python

Python SDK for the [respond jobs API](https://respond.run) — a self-hosted queue, scheduler, and blob store.

## Installation

```toml
# pyproject.toml
[project]
dependencies = ["respond-client>=0.3.0"]

[tool.uv.sources]
respond-client = { git = "https://github.com/respond-run/respond-client-python.git", tag = "v0.3.0" }
```

## Concepts

respond is a job server. Your code talks to it in two roles:

- **Producer** — submits jobs (an API server, a cron trigger, a script)
- **Worker** — claims jobs one at a time, does the work, posts results back

Jobs have a `kind` (e.g. `"pdf.convert"`) and an optional JSON `payload`. The server holds each job in a queue until a worker claims a **lease**. The worker owns the job exclusively for the duration of the lease; it must either complete or fail the job before the lease expires, or send heartbeats to extend it.

```
Producer                         respond server                 Worker
   |                                   |                           |
   |-- POST /v1/jobs (kind, payload) -->|                           |
   |                                   |                           |
   |                                   |<-- POST /v1/leases -------|
   |                                   |-- job + lease_token ----->|
   |                                   |                           |-- do work
   |                                   |<-- POST .../heartbeat ----|
   |                                   |<-- POST .../complete -----|
   |                                   |                           |
   |-- GET /v1/jobs/{id} ------------->|                           |
   |<-- {status: "completed", result} -|                           |
```

## Quick start

### Producer side

```python
from respond_client import RespondClient

c = RespondClient("https://respond.run", api_key="ak_live_anon_...")

# Submit a job — returns immediately; the worker runs it asynchronously
job = c.submit(
    "pdf.convert",
    queue="default",
    payload={"source_url": "https://example.com/doc.pdf", "format": "markdown"},
    max_attempts=3,          # retry up to 3 times on transient failure
    timeout_seconds=300,     # worker must complete/heartbeat within this window
    ttl_seconds=86400,       # auto-delete job record after 24 h
)
job_id = job["job_id"]

# Poll for the result (or use a webhook / schedule your own poller)
import time
while True:
    job = c.get_job(job_id)
    if job["status"] in ("completed", "failed"):
        break
    time.sleep(2)

if job["status"] == "completed":
    print(job["result"])       # dict returned by the worker handler
```

### Worker side

The `Worker` class handles the claim → heartbeat → complete/fail loop for you.

```python
from respond_client import Worker
from respond_client.errors import PermanentError

w = Worker(
    base_url="https://respond.run",
    api_key="ak_live_anon_...",
    queues=["default"],          # only claim from these queues
    kinds=["pdf.convert"],       # only claim these job kinds (optional filter)
    lease_seconds=300,           # how long each lease lasts
    heartbeat_interval=60,       # send heartbeat every N seconds automatically
    wait_seconds=30,             # long-poll timeout when the queue is empty
)

@w.handler("pdf.convert")
def handle_convert(ctx, payload):
    url = payload["source_url"]
    fmt = payload.get("format", "markdown")

    # ctx.heartbeat() is sent automatically by the Worker, but you can also
    # call it manually before a slow operation to reset the clock immediately.
    ctx.heartbeat(extend_seconds=300)

    result = convert(url, fmt)   # your actual work here
    return {"content": result}   # dict stored as job.result; visible to producer

w.run()   # blocks; SIGTERM/SIGINT trigger graceful shutdown after current job
```

The handler return value becomes `job["result"]` on the server. Any unhandled exception causes the job to be retried (up to `max_attempts`). Raise `PermanentError` to skip remaining retries immediately.

## Blobs

Use blobs to pass large inputs or outputs that don't fit in the JSON payload (e.g. files, images, audio).

### Upload from the producer side

```python
# Upload a file; get back a blob_id to embed in the job payload
with open("input.pdf", "rb") as f:
    blob = c.upload_blob(f, content_type="application/pdf", ttl_seconds=3600)

job = c.submit("pdf.convert", payload={"blob_id": blob["blob_id"], "format": "markdown"})
```

### Download in the worker

```python
@w.handler("pdf.convert")
def handle(ctx, payload):
    import tempfile, os

    # Stream to disk — no memory spike for large files
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        ctx.download_blob_to_file(payload["blob_id"], tmp.name)

    result_text = convert(tmp.name)
    os.unlink(tmp.name)

    # Clean up input blob once done (optional but keeps storage tidy)
    ctx.delete_blob(payload["blob_id"])

    # Upload large result as a blob instead of embedding it in job.result
    result_blob_id = ctx.upload_blob(result_text.encode(), content_type="text/plain")
    return {"result_blob_id": result_blob_id}
```

### Download the result from the producer side

```python
blob_id = job["result"]["result_blob_id"]
content = c.download_blob(blob_id)   # returns bytes
```

## Schedules

Schedules fire a job automatically on a cron expression or at a specific time. They are idempotent — calling `upsert_schedule` with the same name updates the existing schedule.

```python
# Fire "report.generate" every day at 08:00 UTC
c.upsert_schedule(
    "daily-report",
    kind="report.generate",
    queue="default",
    cron="0 8 * * *",
    payload={"recipients": ["team@example.com"]},
    max_attempts=2,
)

# One-shot: fire once at a specific time
from datetime import datetime, timezone
c.upsert_schedule(
    "launch-email",
    kind="email.send",
    run_at=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
    payload={"template": "launch"},
)

# Pause, resume, or delete
c.pause_schedule("daily-report")
c.resume_schedule("daily-report")
c.delete_schedule("daily-report")
```

## Error handling

```python
from respond_client.errors import (
    RespondError,        # base class
    NotFoundError,       # 404
    AuthError,           # 401 / 403
    LeaseConflictError,  # 409 — lease expired or stolen
    PermanentError,      # raise in a handler to skip retries
)

try:
    job = c.get_job("missing-id")
except NotFoundError:
    print("job not found")
except AuthError:
    print("check your api_key")
except RespondError as e:
    print(f"API error {e.status_code}: {e}")
```

Inside a worker handler, raise `PermanentError` for errors that should never be retried:

```python
@w.handler("pdf.convert")
def handle(ctx, payload):
    if not payload.get("source_url") and not payload.get("blob_id"):
        raise PermanentError("payload must include source_url or blob_id")
    # ...
```

## Async client

All methods have an `a`-prefixed async variant for use in async frameworks (FastAPI, aiohttp, etc.):

```python
import asyncio
from respond_client import RespondClient

async def main():
    async with RespondClient("https://respond.run", api_key="ak_live_anon_...") as c:
        job = await c.asubmit("pdf.convert", payload={"source_url": "..."})
        blob = await c.aupload_blob(open("doc.pdf", "rb"), content_type="application/pdf")

asyncio.run(main())
```

Async methods: `asubmit`, `aget_job`, `aclaim`, `aupload_blob`, `adownload_blob`, `astream_blob`, `adelete_blob`.

## Development

```bash
uv sync --dev
uv run ruff check .
uv run pytest
```
