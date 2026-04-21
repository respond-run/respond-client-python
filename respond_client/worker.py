"""High-level Worker for the respond jobs API.

Usage:
    from respond_client import Worker

    w = Worker(
        base_url="https://respond.run",
        api_key="ak_live_anon_...",
        queues=["default"],
        concurrency=1,
    )

    @w.handler("my.kind")
    def handle(ctx, payload):
        # ctx.heartbeat()              -- extend lease
        # ctx.download_blob(blob_id)   -- fetch a blob by id
        result = do_work(payload)
        return result  # dict or None; stored as job.result

    w.run()  # blocks; SIGTERM/SIGINT trigger graceful shutdown
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from respond_client.client import RespondClient
from respond_client.errors import LeaseConflictError, PermanentError, RespondError

logger = logging.getLogger(__name__)


@dataclass
class JobContext:
    """Context object passed to each handler.

    Provides helpers that interact with the respond API using the active lease.
    """

    job_id: str
    kind: str
    queue: str
    lease_token: str
    attempts: int
    max_attempts: int
    _client: RespondClient

    def heartbeat(self, extend_seconds: int = 300) -> None:
        """Extend the lease. Call this periodically for long-running jobs.

        Raises LeaseConflictError if the lease has been reclaimed by another
        worker so the handler can stop early instead of silently continuing work
        it no longer owns.
        """
        self._client.heartbeat(self.job_id, self.lease_token, extend_seconds)

    def download_blob(self, blob_id: str) -> bytes:
        """Download a blob by ID, buffering the full content in memory."""
        return self._client.download_blob(blob_id)

    def download_blob_to_file(self, blob_id: str, dest_path: str) -> None:
        """Stream a blob directly to *dest_path* without buffering in memory."""
        self._client.stream_blob_to_file(blob_id, dest_path)

    def upload_blob(self, data: bytes, content_type: str = "application/octet-stream") -> str:
        """Upload bytes as a blob and return the blob_id."""
        resp = self._client.upload_blob(data, content_type=content_type)
        return resp["blob_id"]

    def delete_blob(self, blob_id: str) -> None:
        """Delete a blob by ID."""
        self._client.delete_blob(blob_id)


class Worker:
    """SIGTERM-aware job worker with automatic lease heartbeating.

    One handler can be registered per ``kind`` with the ``@w.handler()``
    decorator.  The worker runs a tight poll loop, claiming one job at a time,
    running the handler in a thread (so the heartbeat loop can fire on the main
    thread), and marking the job complete or failed.

    Exponential backoff is applied when the API is unreachable.
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        queues: list[str] | None = None,
        kinds: list[str] | None = None,
        lease_seconds: int = 300,
        heartbeat_interval: int = 60,
        wait_seconds: int = 30,
        concurrency: int = 1,  # currently 1 is the only supported value
    ):
        self._client = RespondClient(base_url, api_key=api_key, timeout=wait_seconds + 15)
        self._queues = queues
        self._kinds = kinds
        self._lease_seconds = lease_seconds
        self._heartbeat_interval = heartbeat_interval
        self._wait_seconds = wait_seconds
        self._handlers: dict[str, Callable] = {}
        self._running = True

    def handler(self, kind: str):
        """Decorator to register a handler for a given job kind."""
        def decorator(fn: Callable):
            self._handlers[kind] = fn
            return fn
        return decorator

    def _handle_signal(self, signum, frame):
        logger.info("Received signal %s, finishing current job then stopping…", signum)
        self._running = False

    def _process(self, job: dict) -> None:
        job_id = job["job_id"]
        kind = job["kind"]
        lease_token = job["lease_token"]
        payload = job.get("payload") or {}

        handler = self._handlers.get(kind)
        if handler is None:
            logger.warning("No handler registered for kind=%r; failing job %s", kind, job_id)
            try:
                self._client.fail(
                    job_id, lease_token, f"No handler for kind={kind!r}", retryable=False
                )
            except RespondError:
                pass
            return

        ctx = JobContext(
            job_id=job_id,
            kind=kind,
            queue=job["queue"],
            lease_token=lease_token,
            attempts=job["attempts"],
            max_attempts=job.get("max_attempts", 3),
            _client=self._client,
        )

        result_holder: dict = {}
        exc_holder: dict = {}

        def _run():
            try:
                result_holder["result"] = handler(ctx, payload)
            except Exception as exc:
                exc_holder["exc"] = exc
                exc_holder["exc_info"] = sys.exc_info()

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        # Heartbeat on the main thread while the handler runs.
        heartbeat_at = time.monotonic() + self._heartbeat_interval
        while thread.is_alive():
            thread.join(timeout=1.0)
            if time.monotonic() >= heartbeat_at and thread.is_alive():
                try:
                    self._client.heartbeat(job_id, lease_token, self._lease_seconds)
                    heartbeat_at = time.monotonic() + self._heartbeat_interval
                except LeaseConflictError:
                    logger.warning("Lease expired during heartbeat: job_id=%s", job_id)
                    thread.join(timeout=job.get("timeout_seconds", 300))
                    if thread.is_alive():
                        logger.warning(
                            "Handler thread did not exit after lease conflict; "
                            "abandoning daemon thread: job_id=%s",
                            job_id,
                        )
                    return
                except RespondError as exc:
                    logger.warning("Heartbeat error (will retry): %s", exc)

        if "exc" in exc_holder:
            exc = exc_holder["exc"]
            retryable = not isinstance(exc, PermanentError)
            logger.error(
                "Handler error for job %s (retryable=%s): %s",
                job_id, retryable, exc, exc_info=exc_holder["exc_info"]
            )
            try:
                self._client.fail(job_id, lease_token, str(exc), retryable=retryable)
            except LeaseConflictError:
                logger.warning("Lease expired before fail ack: job_id=%s", job_id)
            except RespondError as ack_exc:
                logger.error("Could not send fail ack: %s", ack_exc)
            return

        result = result_holder.get("result")
        if not isinstance(result, dict):
            result = {"value": result} if result is not None else None

        try:
            self._client.complete(job_id, lease_token, result=result)
            logger.info("Completed job %s (kind=%s)", job_id, kind)
        except LeaseConflictError:
            logger.warning("Lease expired before complete ack: job_id=%s", job_id)
        except RespondError as exc:
            logger.error("Could not send complete ack: %s", exc)

    def run(self) -> None:
        """Block and process jobs until SIGTERM/SIGINT."""
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        logger.info(
            "Worker started (queues=%s, kinds=%s, lease=%ds)",
            self._queues,
            self._kinds,
            self._lease_seconds,
        )

        backoff = 1.0
        _MAX_BACKOFF = 60.0

        try:
            while self._running:
                try:
                    job = self._client.claim(
                        queues=self._queues,
                        kinds=self._kinds,
                        wait_seconds=self._wait_seconds,
                        lease_seconds=self._lease_seconds,
                    )
                    backoff = 1.0  # reset on success

                    if job is None:
                        continue  # long-poll returned 204, loop again

                    self._process(job)

                except RespondError as exc:
                    logger.error("API error: %s (backoff %.1fs)", exc, backoff)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, _MAX_BACKOFF)
                except Exception as exc:
                    logger.exception("Unexpected error: %s (backoff %.1fs)", exc, backoff)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, _MAX_BACKOFF)
        finally:
            self._client.close()

        logger.info("Worker stopped")
