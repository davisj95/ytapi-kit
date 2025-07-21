from __future__ import annotations

import inspect, functools
import collections.abc as _abc
import pandas as pd

from typing import Iterable, Any, Sequence, get_origin, get_args, Union, get_type_hints, Mapping

from collections.abc import Sequence as ABCSequence

__all__ = [
    "_string_to_tuple",
    "_raise_invalid_argument",
    "runtime_typecheck",
    "_validate_enum",
    "_prune_none",
    "_paged_list"
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

def _is_instance(val: Any, anno: Any) -> bool:

    origin = get_origin(anno)

    if origin is ABCSequence and isinstance(val, (str, bytes)):
        return False

    if origin is None:
        return anno is Any or isinstance(val, anno)

    if origin is Union:
        return any(_is_instance(val, arg) for arg in get_args(anno))

    if origin is Sequence and get_args(anno) == (str,):
        return isinstance(val, Sequence) and all(isinstance(v, str) for v in val)

    return isinstance(val, origin)

def runtime_typecheck(fn):

    sig   = inspect.signature(fn)
    hints = get_type_hints(fn)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        bound = sig.bind_partial(*args, **kwargs)
        for name, value in bound.arguments.items():
            anno = hints.get(name)
            if anno and not _is_instance(value, anno):
                raise TypeError(
                    f"{fn.__name__}() argument '{name}' "
                    f"expects {anno}, got {type(value).__name__}"
                )
        return fn(*args, **kwargs)

    return wrapper

def _validate_enum(
    param_name: str,
    value: str | Sequence[str],
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

def _prune_none(mapping: Mapping[str, object]) -> dict[str, object]:
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
