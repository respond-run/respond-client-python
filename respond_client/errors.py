"""Exceptions raised by the respond client SDK."""


class RespondError(Exception):
    """Base class for all respond client errors."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class LeaseConflictError(RespondError):
    """Raised when a lease token is invalid or expired (HTTP 409)."""


class NotFoundError(RespondError):
    """Raised when a resource is not found (HTTP 404)."""


class AuthError(RespondError):
    """Raised on authentication failure (HTTP 401/403)."""


class PermanentError(Exception):
    """Raised by a job handler to signal a non-retryable failure.

    The worker will call fail(..., retryable=False), consuming no further
    retry slots.  Use this for errors where retrying will never succeed
    (e.g. malformed payloads, unsupported formats).
    """
