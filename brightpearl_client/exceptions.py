class BrightpearlError(Exception):
    """Base error for Brightpearl API failures."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class BrightpearlAuthError(BrightpearlError):
    """401/403 — bad app-ref/account-token or missing permission."""


class BrightpearlNotFound(BrightpearlError):
    """404 — resource does not exist."""


class BrightpearlThrottled(BrightpearlError):
    """503 throttle responses that persisted past all retries."""
