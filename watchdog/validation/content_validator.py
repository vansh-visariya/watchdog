from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from watchdog.core.config import FieldRule, ValidationConfig, WatchDogConfig
from watchdog.core.exceptions import InvalidFieldValue, MissingRequiredField
from watchdog.core.models import EventEnvelope, ReasonCode


class ContentValidator:
    def __init__(self, config: WatchDogConfig) -> None:
        self.validation: ValidationConfig = config.validation

    def validate_payload(self, envelope: EventEnvelope) -> list[ReasonCode]:
        violations: list[ReasonCode] = []

        try:
            payload = json.loads(envelope.payload_json)
        except json.JSONDecodeError:
            violations.append(ReasonCode.INVALID_FIELD_VALUE)
            violations.append(ReasonCode.MISSING_REQUIRED_FIELD)
            return violations

        for field_name in self.validation.required_payload_fields:
            if field_name not in payload or payload[field_name] is None:
                violations.append(ReasonCode.MISSING_REQUIRED_FIELD)
                continue

            field_rule = self.validation.field_rules.get(field_name)
            if field_rule:
                value = payload[field_name]
                field_violations = self._check_field(field_name, value, field_rule)
                violations.extend(field_violations)

        return violations

    def _check_field(
        self, field_name: str, value: Any, rule: FieldRule
    ) -> list[ReasonCode]:
        violations: list[ReasonCode] = []

        if rule.type == "number" and not isinstance(value, (int, float)):
            violations.append(ReasonCode.INVALID_FIELD_VALUE)
            return violations

        if rule.min is not None and isinstance(value, (int, float)) and value < rule.min:
            violations.append(ReasonCode.INVALID_FIELD_VALUE)
            return violations

        if rule.accepted_formats and isinstance(value, str):
            if not self._check_timestamp_format(value, rule.accepted_formats):
                violations.append(ReasonCode.INVALID_FIELD_VALUE)

        return violations

    def _check_timestamp_format(self, value: str, formats: list[str]) -> bool:
        for fmt in formats:
            if fmt == "iso8601":
                try:
                    datetime.fromisoformat(value.replace("Z", "+00:00"))
                    return True
                except (ValueError, TypeError):
                    continue
            elif fmt == "epoch_millis":
                try:
                    epoch_ms = int(value)
                    datetime.fromtimestamp(epoch_ms / 1000, tz=UTC)
                    return True
                except (ValueError, TypeError, OSError):
                    continue
        return False
