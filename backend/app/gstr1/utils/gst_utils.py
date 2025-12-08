import re
from typing import Optional


GSTIN_REGEX = re.compile(
    r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$"
)


def safe_string(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        from math import isnan

        if isnan(value):
            return ""
    s = str(value).strip()
    if s.lower() in ("nan", "none"):
        return ""
    return s


def to_float(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float):
        from math import isnan

        if isnan(value):
            return None
        return value
    if isinstance(value, int):
        return float(value)
    if isinstance(value, str):
        stripped = value.replace(",", "").strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def round_money(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 2)


def is_valid_gstin(gstin: str) -> bool:
    if not gstin:
        return False
    return bool(GSTIN_REGEX.match(gstin))
