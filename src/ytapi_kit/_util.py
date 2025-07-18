from __future__ import annotations

import inspect, functools, typing
import collections.abc as _abc
import pandas as pd

__all__ = [
    "_string_to_tuple",
    "_raise_invalid_argument",
    "runtime_typecheck",
    "_validate_enum",
    "_prune_none",
    "_paged_list"
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

def _validate_enum(
    param_name: str,
    value: str | typing.Sequence[str],
    allowed: set[str],
    *,
    allow_multi: bool = True,
) -> tuple[str, ...]:
    """Normalise *value* to a tuple and verify every element is in *allowed*."""

    if isinstance(value, str):
        items = [s.strip() for s in value.split(",")] if allow_multi else [value]
    elif isinstance(value, _abc.Iterable):
        items = list(value)
    else:
        raise TypeError(f"{param_name} must be str or Sequence[str]")

    if not items:
        raise ValueError(f"{param_name} cannot be empty")

    if not set(items).issubset(allowed):
        _raise_invalid_argument(param_name, value, allowed)

    if not allow_multi and len(items) != 1:
        _raise_invalid_argument(param_name, value, allowed)

    return tuple(dict.fromkeys(items))

def _prune_none(mapping: typing.Mapping[str, object]) -> dict[str, object]:
    """Return a new dict without the None-valued keys."""
    return {k: v for k, v in mapping.items() if v is not None}

def _paged_list(fn, **first_call_kwargs) -> pd.DataFrame:
    """
    Generic paginator: keeps calling *fn* until no `nextPageToken`.
    `fn` must return (DataFrame, next_token_or_None).
    """
    dfs, token = fn(**first_call_kwargs)
    frames = [dfs]

    while token:
        page_df, token = fn(page_token=token, **first_call_kwargs)
        frames.append(page_df)

    return pd.concat(frames, ignore_index=True)
