"""Low-level HTTP client for the respond jobs API.

Provides both sync and async methods.  Sync methods are thin wrappers that
run httpx synchronously; async methods use httpx.AsyncClient.

All methods raise :class:`RespondError` subclasses on non-2xx responses.
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


class RespondClient:
    """Synchronous + async low-level client for the respond API.

    Usage (sync):
        c = RespondClient("https://respond.run", api_key="ak_live_anon_...")
        job = c.submit("my.kind", payload={"x": 1})
        status = c.get_job(job["job_id"])

    Usage (async):
        async with c.async_client() as ac:
            job = await ac.submit("my.kind", payload={"x": 1})
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

        resp = self._client.post(self._url("jobs"), json=body)
        _raise_for_status(resp)
        return resp.json()

    def get_job(self, job_id: str) -> dict[str, Any]:
        resp = self._client.get(self._url(f"jobs/{job_id}"))
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
    ) -> dict[str, Any]:
        params: dict = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        if kind:
            params["kind"] = kind
        if queue:
            params["queue"] = queue
        resp = self._client.get(self._url("jobs"), params=params)
        _raise_for_status(resp)
        return resp.json()

    def cancel_job(self, job_id: str) -> None:
        resp = self._client.delete(self._url(f"jobs/{job_id}"))
        _raise_for_status(resp)

    # ── Leases ────────────────────────────────────────────────────────────────

    def claim(
        self,
        *,
        queues: list[str] | None = None,
        kinds: list[str] | None = None,
        wait_seconds: int = 30,
        lease_seconds: int = 300,
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
        resp = self._client.post(self._url("leases"), json=body, timeout=wait_seconds + 10)
        if resp.status_code == 204:
            return None
        _raise_for_status(resp)
        return resp.json()

    def heartbeat(self, job_id: str, lease_token: str, extend_seconds: int = 300) -> None:
        resp = self._client.post(
            self._url(f"jobs/{job_id}/heartbeat"),
            json={"lease_token": lease_token, "extend_seconds": extend_seconds},
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
    ) -> None:
        body: dict = {"lease_token": lease_token}
        if result is not None:
            body["result"] = result
        if result_blob_id:
            body["result_blob_id"] = result_blob_id
        if timings:
            body["timings"] = timings
        resp = self._client.post(self._url(f"jobs/{job_id}/complete"), json=body)
        _raise_for_status(resp)

    def fail(self, job_id: str, lease_token: str, error: str, retryable: bool = True) -> None:
        resp = self._client.post(
            self._url(f"jobs/{job_id}/fail"),
            json={"lease_token": lease_token, "error": error, "retryable": retryable},
        )
        _raise_for_status(resp)

    # ── Blobs ─────────────────────────────────────────────────────────────────

    def upload_blob(
        self,
        data: bytes | io.IOBase,
        *,
        content_type: str = "application/octet-stream",
        filename: str = "blob",
        ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        params: dict = {}
        if ttl_seconds is not None:
            params["ttl_seconds"] = ttl_seconds
        resp = self._client.post(
            self._url("blobs"),
            files={"file": (filename, data, content_type)},
            params=params,
            timeout=httpx.Timeout(connect=10, read=300, write=None, pool=10),
        )
        _raise_for_status(resp)
        return resp.json()

    def download_blob(self, blob_id: str) -> bytes:
        resp = self._client.get(self._url(f"blobs/{blob_id}"), follow_redirects=True, timeout=120)
        _raise_for_status(resp)
        return resp.content

    def stream_blob_to_file(self, blob_id: str, dest_path: str, chunk_size: int = 65536) -> None:
        """Stream a blob directly to *dest_path* without buffering in memory."""
        with self._client.stream(
            "GET", self._url(f"blobs/{blob_id}"), follow_redirects=True, timeout=120
        ) as resp:
            if not resp.is_success:
                resp.read()
                _raise_for_status(resp)
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=chunk_size):
                    f.write(chunk)

    def delete_blob(self, blob_id: str) -> None:
        resp = self._client.delete(self._url(f"blobs/{blob_id}"))
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
        resp = await self._get_aclient().post(self._url("jobs"), json=body)
        _raise_for_status(resp)
        return resp.json()

    async def aget_job(self, job_id: str) -> dict[str, Any]:
        resp = await self._get_aclient().get(self._url(f"jobs/{job_id}"))
        _raise_for_status(resp)
        return resp.json()

    async def aclaim(
        self,
        *,
        queues: list[str] | None = None,
        kinds: list[str] | None = None,
        wait_seconds: int = 30,
        lease_seconds: int = 300,
    ) -> dict[str, Any] | None:
        body: dict = {"wait_seconds": wait_seconds, "lease_seconds": lease_seconds}
        if queues is not None:
            body["queues"] = queues
        if kinds is not None:
            body["kinds"] = kinds
        resp = await self._get_aclient().post(
            self._url("leases"), json=body, timeout=wait_seconds + 10
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
        filename: str = "blob",
        ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        params: dict = {}
        if ttl_seconds is not None:
            params["ttl_seconds"] = ttl_seconds
        resp = await self._get_aclient().post(
            self._url("blobs"),
            files={"file": (filename, data, content_type)},
            params=params,
            timeout=httpx.Timeout(connect=10, read=300, write=None, pool=10),
        )
        _raise_for_status(resp)
        return resp.json()

    async def adownload_blob(self, blob_id: str) -> bytes:
        resp = await self._get_aclient().get(
            self._url(f"blobs/{blob_id}"), follow_redirects=True, timeout=120
        )
        _raise_for_status(resp)
        return resp.content

    async def astream_blob(self, blob_id: str, chunk_size: int = 65536):
        """Stream a blob in chunks without buffering the full content in memory.

        Raises RespondError subclasses on non-2xx responses before yielding any
        data, so callers can detect errors before committing to a 200 response.
        """
        async with self._get_aclient().stream(
            "GET", self._url(f"blobs/{blob_id}"), follow_redirects=True, timeout=120
        ) as resp:
            if not resp.is_success:
                await resp.aread()
                _raise_for_status(resp)
            async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                yield chunk

    async def adelete_blob(self, blob_id: str) -> None:
        resp = await self._get_aclient().delete(self._url(f"blobs/{blob_id}"))
        _raise_for_status(resp)
