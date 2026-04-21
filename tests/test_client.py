"""Unit tests for RespondClient using httpx mock transport."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from respond_client.client import RespondClient
from respond_client.errors import LeaseConflictError, NotFoundError, RespondError


def _now() -> str:
    return datetime.now(UTC).isoformat()


class _MockTransport(httpx.BaseTransport):
    """Programmable mock transport: call .register(method, path, response)."""

    def __init__(self):
        self._routes: list[tuple[str, str, httpx.Response]] = []

    def register(self, method: str, path: str, response: httpx.Response) -> None:
        self._routes.append((method.upper(), path, response))

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        for m, p, resp in self._routes:
            if m == request.method and p == path:
                self._routes.remove((m, p, resp))
                return resp
        raise AssertionError(f"No mock registered for {request.method} {path}")


def _json_resp(data: dict, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=data)


def _job(job_id: str = "job-1", status: str = "pending") -> dict:
    now = _now()
    return {
        "job_id": job_id,
        "kind": "test.kind",
        "queue": "default",
        "status": status,
        "payload": None,
        "result": None,
        "result_blob_id": None,
        "error": None,
        "timings": None,
        "run_at": now,
        "schedule_id": None,
        "attempts": 0,
        "max_attempts": 3,
        "timeout_seconds": 300,
        "idempotency_key": None,
        "created_at": now,
        "updated_at": now,
        "completed_at": None,
        "expires_at": None,
    }


@pytest.fixture()
def transport():
    return _MockTransport()


@pytest.fixture()
def client(transport):
    c = RespondClient.__new__(RespondClient)
    c._base = "http://testserver"
    c._headers = {}
    c._timeout = 10.0
    c._client = httpx.Client(transport=transport, base_url="http://testserver")
    c._aclient = None
    return c


class TestJobs:
    def test_submit(self, client, transport):
        transport.register("POST", "/v1/jobs", _json_resp(_job(), 202))
        job = client.submit("test.kind", payload={"x": 1})
        assert job["kind"] == "test.kind"
        assert job["status"] == "pending"

    def test_get_job(self, client, transport):
        transport.register("GET", "/v1/jobs/job-1", _json_resp(_job("job-1")))
        job = client.get_job("job-1")
        assert job["job_id"] == "job-1"

    def test_get_job_not_found(self, client, transport):
        transport.register(
            "GET", "/v1/jobs/missing", _json_resp({"detail": "not found"}, 404)
        )
        with pytest.raises(NotFoundError):
            client.get_job("missing")

    def test_cancel_job(self, client, transport):
        transport.register("DELETE", "/v1/jobs/job-1", _json_resp({"ok": True}))
        client.cancel_job("job-1")

    def test_list_jobs(self, client, transport):
        transport.register(
            "GET",
            "/v1/jobs",
            _json_resp({"jobs": [_job()], "total": 1}),
        )
        result = client.list_jobs()
        assert result["total"] == 1


class TestLeases:
    def test_claim_returns_job(self, client, transport):
        lease = {**_job(), "lease_token": "tok-1", "lease_expires_at": _now()}
        transport.register("POST", "/v1/leases", _json_resp(lease))
        result = client.claim(wait_seconds=0)
        assert result is not None
        assert result["lease_token"] == "tok-1"

    def test_claim_returns_none_on_204(self, client, transport):
        transport.register("POST", "/v1/leases", httpx.Response(204))
        result = client.claim(wait_seconds=0)
        assert result is None

    def test_heartbeat(self, client, transport):
        transport.register(
            "POST", "/v1/jobs/job-1/heartbeat", _json_resp({"ok": True})
        )
        client.heartbeat("job-1", "tok-1", 300)

    def test_heartbeat_conflict(self, client, transport):
        transport.register(
            "POST",
            "/v1/jobs/job-1/heartbeat",
            _json_resp({"detail": "conflict"}, 409),
        )
        with pytest.raises(LeaseConflictError):
            client.heartbeat("job-1", "tok-1", 300)

    def test_complete(self, client, transport):
        transport.register(
            "POST", "/v1/jobs/job-1/complete", _json_resp({"ok": True})
        )
        client.complete("job-1", "tok-1", result={"done": True})

    def test_fail(self, client, transport):
        transport.register("POST", "/v1/jobs/job-1/fail", _json_resp({"ok": True}))
        client.fail("job-1", "tok-1", "oops", retryable=False)


class TestErrors:
    def test_auth_error_on_401(self, client, transport):
        from respond_client.errors import AuthError

        transport.register(
            "GET", "/v1/jobs/job-1", _json_resp({"detail": "Unauthorized"}, 401)
        )
        with pytest.raises(AuthError):
            client.get_job("job-1")

    def test_generic_error_on_500(self, client, transport):
        transport.register(
            "GET", "/v1/jobs/job-1", _json_resp({"detail": "server error"}, 500)
        )
        with pytest.raises(RespondError):
            client.get_job("job-1")


class TestSchedules:
    def _sched(self) -> dict:
        now = _now()
        return {
            "schedule_id": "sched-1",
            "name": "my-sched",
            "kind": "test.kind",
            "queue": "default",
            "payload": None,
            "cron": "* * * * *",
            "run_at": None,
            "max_attempts": 3,
            "timeout_seconds": 300,
            "job_ttl_seconds": None,
            "paused": False,
            "next_fire_at": None,
            "last_fire_at": None,
            "created_at": now,
            "updated_at": now,
        }

    def test_upsert(self, client, transport):
        transport.register("PUT", "/v1/schedules/my-sched", _json_resp(self._sched()))
        sched = client.upsert_schedule("my-sched", kind="test.kind", cron="* * * * *")
        assert sched["name"] == "my-sched"

    def test_list(self, client, transport):
        transport.register(
            "GET", "/v1/schedules", _json_resp({"schedules": [self._sched()]})
        )
        result = client.list_schedules()
        assert len(result["schedules"]) == 1

    def test_delete(self, client, transport):
        transport.register(
            "DELETE", "/v1/schedules/my-sched", _json_resp({"ok": True})
        )
        client.delete_schedule("my-sched")


class TestAuthTokenOverride:
    """Tests for per-call auth_token kwarg."""

    def _captured_transport(self):
        """Return a transport that records the last request."""
        captured = {}

        class CapturingTransport(httpx.BaseTransport):
            def handle_request(self, request: httpx.Request) -> httpx.Response:
                captured["last"] = request
                return httpx.Response(202, json=_job())

        return CapturingTransport(), captured

    def _client_with_default_key(self, transport) -> RespondClient:
        c = RespondClient.__new__(RespondClient)
        c._base = "http://testserver"
        c._headers = {"Authorization": "Bearer default-key"}
        c._timeout = 10.0
        c._client = httpx.Client(transport=transport, headers=c._headers, base_url="http://testserver")
        c._aclient = None
        return c

    def test_auth_token_sent_on_submit(self):
        transport, captured = self._captured_transport()
        c = RespondClient.__new__(RespondClient)
        c._base = "http://testserver"
        c._headers = {}
        c._timeout = 10.0
        c._client = httpx.Client(transport=transport, base_url="http://testserver")
        c._aclient = None

        c.submit("test.kind", auth_token="caller-token-abc")
        assert captured["last"].headers.get("authorization") == "Bearer caller-token-abc"

    def test_auth_token_overrides_default_api_key(self):
        transport, captured = self._captured_transport()
        c = self._client_with_default_key(transport)

        c.submit("test.kind", auth_token="per-call-token")
        assert captured["last"].headers.get("authorization") == "Bearer per-call-token"

    def test_no_auth_token_falls_back_to_default(self):
        transport, captured = self._captured_transport()
        c = self._client_with_default_key(transport)

        c.submit("test.kind")
        assert captured["last"].headers.get("authorization") == "Bearer default-key"

    def test_auth_token_on_get_job(self, transport, client):
        transport.register("GET", "/v1/jobs/job-1", _json_resp(_job("job-1")))
        client.get_job("job-1", auth_token="tok-xyz")

    def test_auth_token_on_delete_blob(self, transport, client):
        transport.register("DELETE", "/v1/blobs/blob-1", _json_resp({"ok": True}))
        client.delete_blob("blob-1", auth_token="tok-xyz")

    def test_auth_token_on_heartbeat(self, transport, client):
        transport.register("POST", "/v1/jobs/job-1/heartbeat", _json_resp({"ok": True}))
        client.heartbeat("job-1", "lease-tok", 300, auth_token="tok-xyz")

    def test_auth_token_on_complete(self, transport, client):
        transport.register("POST", "/v1/jobs/job-1/complete", _json_resp({"ok": True}))
        client.complete("job-1", "lease-tok", auth_token="tok-xyz")

    def test_auth_token_on_fail(self, transport, client):
        transport.register("POST", "/v1/jobs/job-1/fail", _json_resp({"ok": True}))
        client.fail("job-1", "lease-tok", "oops", auth_token="tok-xyz")

    def test_auth_token_sent_on_blob_upload_alloc_not_put(self):
        """auth_token applies to the allocation POST, not the signed-URL PUT."""
        alloc_captured = {}
        put_captured = {}

        class SplitTransport(httpx.BaseTransport):
            def handle_request(self, request: httpx.Request) -> httpx.Response:
                if request.method == "POST":
                    alloc_captured["auth"] = request.headers.get("authorization")
                    return httpx.Response(200, json={
                        "blob_id": "b-1",
                        "upload_url": "http://testserver/v1/blobs/b-1/upload?token=t&exp=9999999999",
                        "expires_at": _now(),
                    })
                # PUT — signed URL, no auth expected
                put_captured["auth"] = request.headers.get("authorization")
                return httpx.Response(201, json={"blob_id": "b-1", "size_bytes": 5})

        c = RespondClient.__new__(RespondClient)
        c._base = "http://testserver"
        c._headers = {}
        c._timeout = 10.0
        c._client = httpx.Client(transport=SplitTransport(), base_url="http://testserver")
        c._aclient = None

        c.upload_blob(b"hello", auth_token="alloc-token")
        assert alloc_captured.get("auth") == "Bearer alloc-token"
        # The signed-URL PUT carries the Content-Type header but not the auth override.
        assert put_captured.get("auth") is None

    def test_none_auth_token_no_header_added(self):
        transport, captured = self._captured_transport()
        c = RespondClient.__new__(RespondClient)
        c._base = "http://testserver"
        c._headers = {}
        c._timeout = 10.0
        c._client = httpx.Client(transport=transport, base_url="http://testserver")
        c._aclient = None

        c.submit("test.kind", auth_token=None)
        assert "authorization" not in captured["last"].headers
