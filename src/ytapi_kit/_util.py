from __future__ import annotations

from typing import Iterable

__all__ = [
    "_string_to_tuple",
    "_raise_invalid_argument",
    "_check_type"
]

def _string_to_tuple(value: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(value, str):
        # split on commas, trim whitespace, drop empties
        return tuple(s.strip() for s in value.split(",") if s.strip())
    return tuple(value)

def _raise_invalid_argument(param: str, value: str, allowed: Iterable[str]) -> None:
    allowed_set = sorted(set(allowed))
    bullets = "\n  • " + "\n  • ".join(allowed_set)
    raise ValueError(f"{param}={value!r} is invalid. Allowed values:{bullets}")

def _check_type(value, expected, name: str) -> None:
    """Raise TypeError if *value* is not None and not an *expected* type."""
    if value is None:
        return
    if not isinstance(value, expected):
        if isinstance(expected, tuple):
            exp = " or ".join(t.__name__ for t in expected)
        else:
            exp = expected.__name__
        raise TypeError(f"{name} must be {exp} | None")