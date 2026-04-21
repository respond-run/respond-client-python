# respond-client-python

Python SDK for the [respond jobs API](https://respond.run) — a self-hosted, language-agnostic queue, scheduler, and blob store.

## Installation

### pip / uv (PyPI — coming soon)

```bash
pip install respond-client
```

### Git source (recommended until published on PyPI)

```toml
# pyproject.toml
[tool.uv.sources]
respond-client = { git = "ssh://git@github.com/respond-run/respond-client-python.git", tag = "v0.1.0" }
```

## Usage

### Low-level client

```python
from respond_client import RespondClient

c = RespondClient("https://respond.run", api_key="ak_live_anon_...")

# Submit a job
job = c.submit("my.task", queue="default", payload={"url": "https://example.com"})
print(job["job_id"])

# Upload a large payload as a blob
blob = c.upload_blob(open("doc.pdf", "rb"), content_type="application/pdf")

# Check job status
status = c.get_job(job["job_id"])

# Create a recurring schedule
c.upsert_schedule("email-scan", kind="email.scan", cron="*/5 * * * *", payload={"mailbox": "inbox"})
```

### High-level worker

```python
from respond_client import Worker

w = Worker(
    base_url="https://respond.run",
    api_key="ak_live_anon_...",
    queues=["default"],
    lease_seconds=300,
)

@w.handler("my.task")
def handle_task(ctx, payload):
    # ctx.heartbeat()              -- extend lease mid-job
    # ctx.download_blob(blob_id)   -- fetch a blob
    # ctx.upload_blob(data)        -- upload result as blob
    result = do_work(payload["url"])
    return {"output": result}

w.run()  # SIGTERM-aware; blocks until shutdown
```

## Development

```bash
uv sync --dev
uv run ruff check .
uv run pytest
```
