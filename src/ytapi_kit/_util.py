from __future__ import annotations

import inspect, functools, typing

__all__ = [
    "_string_to_tuple",
    "_raise_invalid_argument",
    "runtime_typecheck"
]

def _string_to_tuple(value: str | typing.Iterable[str]) -> tuple[str, ...]:
    if isinstance(value, str):
        # split on commas, trim whitespace, drop empties
        return tuple(s.strip() for s in value.split(",") if s.strip())
    return tuple(value)

def _raise_invalid_argument(param: str, value: str, allowed: typing.Iterable[str]) -> None:
    allowed_set = sorted(set(allowed))
    bullets = "\n  • " + "\n  • ".join(allowed_set)
    raise ValueError(f"{param}={value!r} is invalid. Allowed values:{bullets}")

def runtime_typecheck(func):

    sig   = inspect.signature(func)
    hints = typing.get_type_hints(func)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()

        for name, value in bound.arguments.items():
            if value is None:
                continue
            expected = hints.get(name)
            if expected is None:
                continue

            origin = typing.get_origin(expected)
            allowed = typing.get_args(expected) if origin is typing.Union else (expected,)

            if not isinstance(value, allowed):
                typestr = " or ".join(t.__name__ for t in allowed)
                raise TypeError(f"{name} must be {typestr} | None")

        return func(*args, **kwargs)

    return wrapper