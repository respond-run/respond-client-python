"""Low-level HTTP client for the respond jobs API.

Provides both sync and async methods.  Sync methods are thin wrappers that
run httpx synchronously; async methods use httpx.AsyncClient.

All methods raise :class:`RespondError` subclasses on non-2xx responses.

Blob upload/download uses server-minted signed URLs — the API key is only used
to allocate the URL; the actual byte transfer happens without Bearer auth.
"""

from __future__ import annotations

import io
from datetime import datetime
from typing import Any

import httpx

from respond_client.errors import AuthError, LeaseConflictError, NotFoundError, RespondError


def _raise_for_status(resp: httpx.Response) -> None:
    if resp.is_success:
        return
    detail = resp.text
    try:
        detail = resp.json().get("detail", detail)
    except Exception:
        pass
    sc = resp.status_code
    if sc == 404:
        raise NotFoundError(detail, status_code=sc)
    if sc == 409:
        raise LeaseConflictError(detail, status_code=sc)
    if sc in (401, 403):
        raise AuthError(detail, status_code=sc)
    raise RespondError(detail, status_code=sc)


def _override_headers(auth_token: str | None) -> dict[str, str]:
    """Return a per-request Authorization header dict, or empty if no override."""
    if auth_token:
        return {"Authorization": f"Bearer {auth_token}"}
    return {}


class RespondClient:
    """Synchronous + async low-level client for the respond API.

    Usage (sync):
        c = RespondClient("https://respond.run", api_key="ak_live_anon_...")
        job = c.submit("my.kind", payload={"x": 1})
        status = c.get_job(job["job_id"])

    Usage (async):
        job = await c.asubmit("my.kind", payload={"x": 1})

    Per-call ``auth_token`` override
    ---------------------------------
    Every API method accepts an optional ``auth_token`` keyword argument.  When
    supplied, that token is sent as ``Authorization: Bearer <auth_token>`` for
    that single call, overriding the client-default ``api_key``.  This lets a
    shared, pooled client forward per-request caller credentials without
    rebuilding the client.

    For two-step blob flows the override applies only to the authenticated
    step (the allocation ``POST``); the subsequent signed-URL ``PUT``/``GET``
    is always unauthenticated, matching the server's expectation.
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        timeout: float = 60.0,
    ):
        self._base = base_url.rstrip("/")
        self._headers: dict[str, str] = {}
        if api_key:
            self._headers["Authorization"] = f"Bearer {api_key}"
        self._timeout = timeout
        # Persistent sync client — keeps the connection pool alive across calls.
        self._client = self._sync_client()
        # Persistent async client — created lazily on first async call.
        self._aclient: httpx.AsyncClient | None = None

    def _sync_client(self) -> httpx.Client:
        """Create the underlying sync httpx.Client. Override in tests to inject a transport."""
        return httpx.Client(headers=self._headers, timeout=self._timeout)

    def close(self) -> None:
        """Close the underlying sync HTTP connection pool."""
        self._client.close()

    async def aclose(self) -> None:
        """Close the async connection pool (and the sync pool)."""
        if self._aclient is not None:
            await self._aclient.aclose()
            self._aclient = None
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def _url(self, path: str) -> str:
        return f"{self._base}/v1/{path.lstrip('/')}"

    def _get_aclient(self) -> httpx.AsyncClient:
        """Return the persistent async client, creating it on the first call.

        Safe under asyncio's single-threaded event loop — no two coroutines can
        reach this simultaneously between await points.
        """
        if self._aclient is None:
            self._aclient = httpx.AsyncClient(headers=self._headers, timeout=self._timeout)
        return self._aclient

    # ── Jobs ──────────────────────────────────────────────────────────────────

    def submit(
        self,
        kind: str,
        *,
        queue: str = "default",
        payload: dict | None = None,
        run_at: datetime | None = None,
        max_attempts: int = 3,
        timeout_seconds: int = 300,
        idempotency_key: str | None = None,
        ttl_seconds: int | None = None,
        auth_token: str | None = None,
    ) -> dict[str, Any]:
        body: dict = {
            "kind": kind,
            "queue": queue,
            "payload": payload,
            "max_attempts": max_attempts,
            "timeout_seconds": timeout_seconds,
        }
        if run_at:
            body["run_at"] = run_at.isoformat()
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
        if ttl_seconds is not None:
            body["ttl_seconds"] = ttl_seconds

        resp = self._client.post(self._url("jobs"), json=body, headers=_override_headers(auth_token))
        _raise_for_status(resp)
        return resp.json()

    def get_job(self, job_id: str, *, auth_token: str | None = None) -> dict[str, Any]:
        resp = self._client.get(self._url(f"jobs/{job_id}"), headers=_override_headers(auth_token))
        _raise_for_status(resp)
        return resp.json()

    def list_jobs(
        self,
        *,
        status: str | None = None,
        kind: str | None = None,
        queue: str | None = None,
        limit: int = 100,
        offset: int = 0,
        auth_token: str | None = None,
    ) -> dict[str, Any]:
        params: dict = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        if kind:
            params["kind"] = kind
        if queue:
            params["queue"] = queue
        resp = self._client.get(
            self._url("jobs"), params=params, headers=_override_headers(auth_token)
        )
        _raise_for_status(resp)
        return resp.json()

    def cancel_job(self, job_id: str, *, auth_token: str | None = None) -> None:
        resp = self._client.delete(
            self._url(f"jobs/{job_id}"), headers=_override_headers(auth_token)
        )
        _raise_for_status(resp)

    # ── Leases ────────────────────────────────────────────────────────────────

    def claim(
        self,
        *,
        queues: list[str] | None = None,
        kinds: list[str] | None = None,
        wait_seconds: int = 30,
        lease_seconds: int = 300,
        auth_token: str | None = None,
    ) -> dict[str, Any] | None:
        """Long-poll for the next available job. Returns None if none available."""
        body: dict = {
            "wait_seconds": wait_seconds,
            "lease_seconds": lease_seconds,
        }
        if queues is not None:
            body["queues"] = queues
        if kinds is not None:
            body["kinds"] = kinds
        resp = self._client.post(
            self._url("leases"),
            json=body,
            timeout=wait_seconds + 10,
            headers=_override_headers(auth_token),
        )
        if resp.status_code == 204:
            return None
        _raise_for_status(resp)
        return resp.json()

    def heartbeat(
        self,
        job_id: str,
        lease_token: str,
        extend_seconds: int = 300,
        *,
        auth_token: str | None = None,
    ) -> None:
        resp = self._client.post(
            self._url(f"jobs/{job_id}/heartbeat"),
            json={"lease_token": lease_token, "extend_seconds": extend_seconds},
            headers=_override_headers(auth_token),
        )
        _raise_for_status(resp)

    def complete(
        self,
        job_id: str,
        lease_token: str,
        *,
        result: dict | None = None,
        result_blob_id: str | None = None,
        timings: dict | None = None,
        auth_token: str | None = None,
    ) -> None:
        body: dict = {"lease_token": lease_token}
        if result is not None:
            body["result"] = result
        if result_blob_id:
            body["result_blob_id"] = result_blob_id
        if timings:
            body["timings"] = timings
        resp = self._client.post(
            self._url(f"jobs/{job_id}/complete"), json=body, headers=_override_headers(auth_token)
        )
        _raise_for_status(resp)

    def fail(
        self,
        job_id: str,
        lease_token: str,
        error: str,
        retryable: bool = True,
        *,
        auth_token: str | None = None,
    ) -> None:
        resp = self._client.post(
            self._url(f"jobs/{job_id}/fail"),
            json={"lease_token": lease_token, "error": error, "retryable": retryable},
            headers=_override_headers(auth_token),
        )
        _raise_for_status(resp)

    # ── Blobs ─────────────────────────────────────────────────────────────────

    def upload_blob(
        self,
        data: bytes | io.IOBase,
        *,
        content_type: str = "application/octet-stream",
        ttl_seconds: int | None = None,
        auth_token: str | None = None,
    ) -> dict[str, Any]:
        """Upload bytes or a file-like object as a blob.

        Internally uses the two-step signed-URL flow:
        1. POST /v1/blobs/uploads  → get signed PUT URL  (uses auth_token if provided)
        2. PUT upload_url          → stream bytes (no Authorization header)

        Returns the blob metadata dict with at minimum ``blob_id``.
        """
        alloc_body: dict = {"content_type": content_type}
        if ttl_seconds is not None:
            alloc_body["ttl_seconds"] = ttl_seconds

        alloc_resp = self._client.post(
            self._url("blobs/uploads"), json=alloc_body, headers=_override_headers(auth_token)
        )
        _raise_for_status(alloc_resp)
        alloc = alloc_resp.json()

        upload_url = alloc["upload_url"]
        if isinstance(data, (bytes, bytearray)):
            body_data = data
        else:
            body_data = data.read()  # type: ignore[union-attr]

        put_resp = self._client.put(
            upload_url,
            content=body_data,
            headers={"Content-Type": content_type},
            timeout=httpx.Timeout(connect=10, read=300, write=None, pool=10),
        )
        _raise_for_status(put_resp)
        blob = put_resp.json()
        # Ensure blob_id is present (server returns it; fall back to allocated id)
        blob.setdefault("blob_id", alloc["blob_id"])
        return blob

    def _get_download_url(self, blob_id: str, *, auth_token: str | None = None) -> str:
        """Mint a signed download URL via the server."""
        resp = self._client.post(
            self._url(f"blobs/{blob_id}/downloads"), headers=_override_headers(auth_token)
        )
        _raise_for_status(resp)
        return resp.json()["download_url"]

    def download_blob(self, blob_id: str, *, auth_token: str | None = None) -> bytes:
        url = self._get_download_url(blob_id, auth_token=auth_token)
        resp = self._client.get(url, follow_redirects=True, timeout=120)
        _raise_for_status(resp)
        return resp.content

    def stream_blob_to_file(
        self,
        blob_id: str,
        dest_path: str,
        chunk_size: int = 65536,
        *,
        auth_token: str | None = None,
    ) -> None:
        """Stream a blob directly to *dest_path* without buffering in memory."""
        url = self._get_download_url(blob_id, auth_token=auth_token)
        with self._client.stream("GET", url, follow_redirects=True, timeout=120) as resp:
            if not resp.is_success:
                resp.read()
                _raise_for_status(resp)
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=chunk_size):
                    f.write(chunk)

    def delete_blob(self, blob_id: str, *, auth_token: str | None = None) -> None:
        resp = self._client.delete(
            self._url(f"blobs/{blob_id}"), headers=_override_headers(auth_token)
        )
        _raise_for_status(resp)

    # ── Schedules ─────────────────────────────────────────────────────────────

    def upsert_schedule(self, name: str, **kwargs) -> dict[str, Any]:
        resp = self._client.put(self._url(f"schedules/{name}"), json=kwargs)
        _raise_for_status(resp)
        return resp.json()

    def list_schedules(self) -> dict[str, Any]:
        resp = self._client.get(self._url("schedules"))
        _raise_for_status(resp)
        return resp.json()

    def delete_schedule(self, name: str) -> None:
        resp = self._client.delete(self._url(f"schedules/{name}"))
        _raise_for_status(resp)

    def pause_schedule(self, name: str) -> dict[str, Any]:
        resp = self._client.post(self._url(f"schedules/{name}/pause"))
        _raise_for_status(resp)
        return resp.json()

    def resume_schedule(self, name: str) -> dict[str, Any]:
        resp = self._client.post(self._url(f"schedules/{name}/resume"))
        _raise_for_status(resp)
        return resp.json()

    # ── Async versions ────────────────────────────────────────────────────────

    async def asubmit(
        self,
        kind: str,
        *,
        queue: str = "default",
        payload: dict | None = None,
        run_at: datetime | None = None,
        max_attempts: int = 3,
        timeout_seconds: int = 300,
        idempotency_key: str | None = None,
        ttl_seconds: int | None = None,
        auth_token: str | None = None,
    ) -> dict[str, Any]:
        body: dict = {
            "kind": kind,
            "queue": queue,
            "payload": payload,
            "max_attempts": max_attempts,
            "timeout_seconds": timeout_seconds,
        }
        if run_at:
            body["run_at"] = run_at.isoformat()
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
        if ttl_seconds is not None:
            body["ttl_seconds"] = ttl_seconds
        resp = await self._get_aclient().post(
            self._url("jobs"), json=body, headers=_override_headers(auth_token)
        )
        _raise_for_status(resp)
        return resp.json()

    async def aget_job(self, job_id: str, *, auth_token: str | None = None) -> dict[str, Any]:
        resp = await self._get_aclient().get(
            self._url(f"jobs/{job_id}"), headers=_override_headers(auth_token)
        )
        _raise_for_status(resp)
        return resp.json()

    async def aclaim(
        self,
        *,
        queues: list[str] | None = None,
        kinds: list[str] | None = None,
        wait_seconds: int = 30,
        lease_seconds: int = 300,
        auth_token: str | None = None,
    ) -> dict[str, Any] | None:
        body: dict = {"wait_seconds": wait_seconds, "lease_seconds": lease_seconds}
        if queues is not None:
            body["queues"] = queues
        if kinds is not None:
            body["kinds"] = kinds
        resp = await self._get_aclient().post(
            self._url("leases"),
            json=body,
            timeout=wait_seconds + 10,
            headers=_override_headers(auth_token),
        )
        if resp.status_code == 204:
            return None
        _raise_for_status(resp)
        return resp.json()

    async def aupload_blob(
        self,
        data: bytes | io.IOBase,
        *,
        content_type: str = "application/octet-stream",
        ttl_seconds: int | None = None,
        auth_token: str | None = None,
    ) -> dict[str, Any]:
        """Async two-step signed-URL upload.

        *auth_token* applies only to the allocation ``POST``; the signed-URL
        ``PUT`` is always unauthenticated.
        """
        alloc_body: dict = {"content_type": content_type}
        if ttl_seconds is not None:
            alloc_body["ttl_seconds"] = ttl_seconds

        alloc_resp = await self._get_aclient().post(
            self._url("blobs/uploads"), json=alloc_body, headers=_override_headers(auth_token)
        )
        _raise_for_status(alloc_resp)
        alloc = alloc_resp.json()

        upload_url = alloc["upload_url"]
        if isinstance(data, (bytes, bytearray)):
            body_data = data
        else:
            body_data = data.read()  # type: ignore[union-attr]

        put_resp = await self._get_aclient().put(
            upload_url,
            content=body_data,
            headers={"Content-Type": content_type},
            timeout=httpx.Timeout(connect=10, read=300, write=None, pool=10),
        )
        _raise_for_status(put_resp)
        blob = put_resp.json()
        blob.setdefault("blob_id", alloc["blob_id"])
        return blob

    async def _aget_download_url(
        self, blob_id: str, *, auth_token: str | None = None
    ) -> str:
        resp = await self._get_aclient().post(
            self._url(f"blobs/{blob_id}/downloads"), headers=_override_headers(auth_token)
        )
        _raise_for_status(resp)
        return resp.json()["download_url"]

    async def adownload_blob(
        self, blob_id: str, *, auth_token: str | None = None
    ) -> bytes:
        url = await self._aget_download_url(blob_id, auth_token=auth_token)
        resp = await self._get_aclient().get(url, follow_redirects=True, timeout=120)
        _raise_for_status(resp)
        return resp.content

    async def astream_blob(
        self, blob_id: str, chunk_size: int = 65536, *, auth_token: str | None = None
    ):
        """Stream a blob in chunks without buffering the full content in memory.

        Raises RespondError subclasses on non-2xx responses before yielding any
        data, so callers can detect errors before committing to a 200 response.

        *auth_token* applies only to the ``POST /blobs/{id}/downloads``
        allocation; the subsequent signed-URL ``GET`` is unauthenticated.
        """
        url = await self._aget_download_url(blob_id, auth_token=auth_token)
        async with self._get_aclient().stream(
            "GET", url, follow_redirects=True, timeout=120
        ) as resp:
            if not resp.is_success:
                await resp.aread()
                _raise_for_status(resp)
            async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                yield chunk

    async def adelete_blob(self, blob_id: str, *, auth_token: str | None = None) -> None:
        resp = await self._get_aclient().delete(
            self._url(f"blobs/{blob_id}"), headers=_override_headers(auth_token)
        )
        _raise_for_status(resp)
