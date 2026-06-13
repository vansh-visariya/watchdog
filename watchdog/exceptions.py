class WatchDogError(Exception):
    """Base exception for WatchDog validation pipeline."""


class SchemaViolation(WatchDogError):
    """Avro envelope deserialization or schema mismatch."""


class MissingRequiredField(WatchDogError):
    """Business payload is missing a required field."""


class InvalidFieldValue(WatchDogError):
    """Business payload field value fails type or range check."""


class NullRateExceeded(WatchDogError):
    """Per-batch null rate exceeds configured threshold."""


class SchemaViolationRateExceeded(WatchDogError):
    """Per-batch schema violation rate exceeds configured threshold."""


class LatenessExceeded(WatchDogError):
    """Event arrived outside the allowed lateness window."""


class HaltError(WatchDogError):
    """Circuit breaker is open — pipeline processing must stop."""


class ConfigError(WatchDogError):
    """Configuration validation failure."""
