from __future__ import annotations

from typing import Iterable

__all__ = [
    "_string_to_tuple",
    "_raise_invalid_argument",
]

def _string_to_tuple(dims: str | Iterable[str]) -> tuple[str, ...]:
    return (dims,) if isinstance(dims, str) else tuple(dims)

def _raise_invalid_argument(param: str, value: str, allowed: Iterable[str]) -> None:
    allowed_set = sorted(set(allowed))
    bullets = "\n  • " + "\n  • ".join(allowed_set)
    raise ValueError(f"{param}={value!r} is invalid. Allowed values:{bullets}")