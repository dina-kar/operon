"""Collection schemas: typed fields, dense vectors and sparse vectors (plan M1.6 Task 3).

The wire form is `_wire.encode_schema` / `_wire.decode_schema`; these types
know nothing about JSON.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, TypeAlias

__all__ = [
    "Distance",
    "Dynamic",
    "Field",
    "Kind",
    "Schema",
    "SparseModifier",
    "SparseVectorField",
    "Text",
    "Vector",
    "boolean",
    "date",
    "f64",
    "i64",
    "json_field",
    "keyword",
    "sparse_vector",
    "text",
    "uuid_field",
    "vector",
]

Distance: TypeAlias = Literal["cosine", "dot", "euclid", "manhattan"]
SparseModifier: TypeAlias = Literal["none", "idf"]
Dynamic: TypeAlias = Literal["strict", "ignore", "map"]

_DISTANCES = ("cosine", "dot", "euclid", "manhattan")
_MODIFIERS = ("none", "idf")
_MAX_DIM = 65535


@dataclass(frozen=True, slots=True)
class Text:
    """A full-text field's kind."""

    analyzer: str = "standard"
    positions: bool = True


Kind: TypeAlias = Text | Literal["keyword", "i64", "f64", "bool", "date", "uuid", "json"]


@dataclass(frozen=True, slots=True)
class Field:
    """A typed field; `source_path` defaults to `name` on the wire."""

    name: str
    kind: Kind
    source_path: str | None = None
    indexed: bool = True
    fast: bool = False


@dataclass(frozen=True, slots=True)
class Vector:
    """A dense vector field."""

    name: str
    dim: int
    distance: Distance = "cosine"


@dataclass(frozen=True, slots=True)
class SparseVectorField:
    """A sparse vector field (overview A26)."""

    name: str
    modifier: SparseModifier = "none"


@dataclass(frozen=True, slots=True)
class Schema:
    """A collection's schema. `version` is set on schemas read from the server."""

    fields: Sequence[Field] = ()
    vectors: Sequence[Vector] = ()
    sparse_vectors: Sequence[SparseVectorField] = ()
    dynamic: Dynamic = "strict"
    max_fields: int = 1000
    version: int | None = None


def text(
    name: str,
    *,
    analyzer: str = "standard",
    positions: bool = True,
    source_path: str | None = None,
) -> Field:
    """A full-text field (not fast)."""
    return Field(name, Text(analyzer, positions), source_path)


def keyword(name: str, *, fast: bool = True, source_path: str | None = None) -> Field:
    return Field(name, "keyword", source_path, fast=fast)


def i64(name: str, *, fast: bool = True, source_path: str | None = None) -> Field:
    return Field(name, "i64", source_path, fast=fast)


def f64(name: str, *, fast: bool = True, source_path: str | None = None) -> Field:
    return Field(name, "f64", source_path, fast=fast)


def boolean(name: str, *, fast: bool = True, source_path: str | None = None) -> Field:
    return Field(name, "bool", source_path, fast=fast)


def date(name: str, *, fast: bool = True, source_path: str | None = None) -> Field:
    return Field(name, "date", source_path, fast=fast)


def uuid_field(name: str, *, source_path: str | None = None) -> Field:
    return Field(name, "uuid", source_path)


def json_field(name: str, *, source_path: str | None = None) -> Field:
    return Field(name, "json", source_path)


def vector(name: str, dim: int, distance: str = "cosine") -> Vector:
    """A dense vector field; `ValueError` unless `1 <= dim <= 65535` and the distance is known."""
    if isinstance(dim, bool) or not isinstance(dim, int) or not 1 <= dim <= _MAX_DIM:
        raise ValueError(f"vector {name!r}: dim must be an int in 1..{_MAX_DIM}, got {dim!r}")
    if distance not in _DISTANCES:
        raise ValueError(f"vector {name!r}: distance must be one of {_DISTANCES}, got {distance!r}")
    return Vector(name, dim, distance)  # type: ignore[arg-type]


def sparse_vector(name: str, *, modifier: str = "none") -> SparseVectorField:
    """A sparse vector field; `ValueError` for an empty name or an unknown modifier."""
    if not name:
        raise ValueError("a sparse vector needs a name")
    if modifier not in _MODIFIERS:
        raise ValueError(f"sparse vector {name!r}: modifier must be 'none' or 'idf'")
    return SparseVectorField(name, modifier)  # type: ignore[arg-type]
