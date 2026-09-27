"""The hybrid query IR (overview §6.6) as frozen dataclasses, with constructors and a builder.

Exported as `operon.q`::

    from operon import q
    ns.search("kb").retrieve(q.vector("embedding", emb, k=50), q.text("refund", k=50)).limit(10)

The wire form is `_wire.encode_search_request`; these types know nothing about JSON.
"""

from __future__ import annotations

import copy
import datetime
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Literal, TypeAlias, TypeVar

from .types import Consistency, Id, SparseVector, VectorLike

__all__ = [
    "AnnParams",
    "Bool",
    "Boost",
    "ConstantScore",
    "Dbsf",
    "Exists",
    "FieldSort",
    "FieldValue",
    "FusedRetriever",
    "Fusion",
    "Fuzziness",
    "Fuzzy",
    "Ids",
    "IsEmpty",
    "IsNull",
    "Match",
    "MatchAll",
    "MatchNone",
    "MatchPhrase",
    "MultiMatch",
    "PkSort",
    "Prefix",
    "Projection",
    "Query",
    "QueryString",
    "Range",
    "RescoreRetriever",
    "Retriever",
    "Rrf",
    "ScoreSort",
    "SearchRequest",
    "SortKey",
    "SourcePaths",
    "SparseRetriever",
    "Term",
    "Terms",
    "TextRetriever",
    "UpTo",
    "ValuesCount",
    "VectorRetriever",
    "WeightedSum",
    "Wildcard",
    "bool_",
    "boost",
    "constant_score",
    "dbsf",
    "exists",
    "field_sort",
    "fused",
    "fuzzy",
    "ids",
    "is_empty",
    "is_null",
    "match",
    "match_all",
    "match_none",
    "match_phrase",
    "multi_match",
    "pk_sort",
    "prefix",
    "query_string",
    "range_",
    "rescore",
    "rrf",
    "score_sort",
    "sparse",
    "term",
    "terms",
    "text",
    "values_count",
    "vector",
    "weighted_sum",
    "wildcard",
]

FieldValue: TypeAlias = str | int | float | bool | datetime.datetime | datetime.date
"""A field value: strings, ints (up to 2**64 - 1), floats, bools and dates (aware datetimes)."""

Fuzziness: TypeAlias = Literal["auto", 0, 1, 2]
Operator: TypeAlias = Literal["or", "and"]
Order: TypeAlias = Literal["asc", "desc"]
MultiMatchKind: TypeAlias = Literal[
    "best_fields", "most_fields", "cross_fields", "phrase", "phrase_prefix"
]

# ---------------------------------------------------------------- queries


@dataclass(frozen=True, slots=True)
class MatchAll:
    pass


@dataclass(frozen=True, slots=True)
class MatchNone:
    pass


@dataclass(frozen=True, slots=True)
class Match:
    field: str
    text: str
    operator: Operator = "or"
    minimum_should_match: str | None = None
    fuzziness: Fuzziness | None = None
    analyzer: str | None = None


@dataclass(frozen=True, slots=True)
class MatchPhrase:
    field: str
    text: str
    slop: int = 0


@dataclass(frozen=True, slots=True)
class MultiMatch:
    fields: Sequence[tuple[str, float]]
    text: str
    kind: MultiMatchKind = "best_fields"
    operator: Operator = "or"
    tie_breaker: float | None = None


@dataclass(frozen=True, slots=True)
class Term:
    field: str
    value: FieldValue


@dataclass(frozen=True, slots=True)
class Terms:
    field: str
    values: Sequence[FieldValue]


@dataclass(frozen=True, slots=True)
class Range:
    field: str
    gt: FieldValue | None = None
    gte: FieldValue | None = None
    lt: FieldValue | None = None
    lte: FieldValue | None = None


@dataclass(frozen=True, slots=True)
class Exists:
    field: str


@dataclass(frozen=True, slots=True)
class IsNull:
    field: str


@dataclass(frozen=True, slots=True)
class IsEmpty:
    field: str


@dataclass(frozen=True, slots=True)
class ValuesCount:
    field: str
    gt: int | None = None
    gte: int | None = None
    lt: int | None = None
    lte: int | None = None


@dataclass(frozen=True, slots=True)
class Prefix:
    field: str
    value: str


@dataclass(frozen=True, slots=True)
class Wildcard:
    field: str
    pattern: str


@dataclass(frozen=True, slots=True)
class Fuzzy:
    field: str
    value: str
    fuzziness: Fuzziness = "auto"


@dataclass(frozen=True, slots=True)
class Ids:
    ids: Sequence[Id]


@dataclass(frozen=True, slots=True)
class QueryString:
    query: str
    default_fields: Sequence[str] = ()
    default_operator: Operator = "or"


@dataclass(frozen=True, slots=True)
class Bool:
    must: Sequence[Query] = ()
    should: Sequence[Query] = ()
    must_not: Sequence[Query] = ()
    filter: Sequence[Query] = ()
    minimum_should_match: str | None = None


@dataclass(frozen=True, slots=True)
class Boost:
    query: Query
    boost: float


@dataclass(frozen=True, slots=True)
class ConstantScore:
    query: Query
    score: float


Query: TypeAlias = (
    MatchAll
    | MatchNone
    | Match
    | MatchPhrase
    | MultiMatch
    | Term
    | Terms
    | Range
    | Exists
    | IsNull
    | IsEmpty
    | ValuesCount
    | Prefix
    | Wildcard
    | Fuzzy
    | Ids
    | QueryString
    | Bool
    | Boost
    | ConstantScore
)

# ---------------------------------------------------------------- retrievers and fusion


@dataclass(frozen=True, slots=True)
class Rrf:
    k: int = 60


@dataclass(frozen=True, slots=True)
class Dbsf:
    pass


@dataclass(frozen=True, slots=True)
class WeightedSum:
    weights: Sequence[float]


Fusion: TypeAlias = Rrf | Dbsf | WeightedSum


@dataclass(frozen=True, slots=True)
class AnnParams:
    """ANN search parameters; `None` leaves each to the server."""

    exact: bool = False
    nprobes: int | None = None
    refine_factor: int | None = None
    ef: int | None = None
    oversampling: float | None = None


@dataclass(frozen=True, slots=True)
class VectorRetriever:
    field: str
    query: VectorLike
    k: int
    params: AnnParams = AnnParams()
    filter: Query | None = None


@dataclass(frozen=True, slots=True)
class TextRetriever:
    query: Query
    k: int


@dataclass(frozen=True, slots=True)
class FusedRetriever:
    inputs: Sequence[Retriever]
    fusion: Fusion
    k: int


@dataclass(frozen=True, slots=True)
class RescoreRetriever:
    input: Retriever
    field: str
    query: VectorLike
    k: int


@dataclass(frozen=True, slots=True)
class SparseRetriever:
    field: str
    query: SparseVector
    k: int
    filter: Query | None = None
    idf_corpus: Query | None = None


Retriever: TypeAlias = (
    VectorRetriever | TextRetriever | FusedRetriever | RescoreRetriever | SparseRetriever
)

# ---------------------------------------------------------------- sort, projection, request


@dataclass(frozen=True, slots=True)
class ScoreSort:
    order: Order = "desc"


@dataclass(frozen=True, slots=True)
class FieldSort:
    field: str
    order: Order = "asc"
    missing: Literal["first", "last"] = "last"


@dataclass(frozen=True, slots=True)
class PkSort:
    order: Order = "asc"


SortKey: TypeAlias = ScoreSort | FieldSort | PkSort


@dataclass(frozen=True, slots=True)
class SourcePaths:
    include: Sequence[str] = ()
    exclude: Sequence[str] = ()


@dataclass(frozen=True, slots=True)
class Projection:
    """What a read returns: the source (all, none or paths), vectors by name, typed fields."""

    source: Literal["all", "none"] | SourcePaths = "all"
    vectors: Sequence[str] = ()
    fields: Sequence[str] = ()


@dataclass(frozen=True, slots=True)
class UpTo:
    n: int


@dataclass(frozen=True, slots=True)
class SearchRequest:
    collection: str
    retrievers: Sequence[Retriever] = ()
    fusion: Fusion | None = None
    filter: Query | None = None
    sort: Sequence[SortKey] = ()
    offset: int = 0
    limit: int = 10
    search_after: Sequence[Any] | None = None
    score_threshold: float | None = None
    select: Projection = field(default_factory=Projection)
    aggregations: Mapping[str, Any] | None = None
    highlight: Mapping[str, Any] | None = None
    group_by: Mapping[str, Any] | None = None
    track_total_hits: Literal["none", "exact"] | UpTo = "none"


def _check_k(retriever: Retriever) -> None:
    if isinstance(retriever.k, bool) or not isinstance(retriever.k, int) or retriever.k < 1:
        raise ValueError(f"a retriever's k must be an int >= 1, got {retriever.k!r}")
    if isinstance(retriever, FusedRetriever):
        for inner in retriever.inputs:
            _check_k(inner)
    elif isinstance(retriever, RescoreRetriever):
        _check_k(retriever.input)


def check_request(request: SearchRequest) -> None:
    """`ValueError` unless `limit >= 1`, `offset >= 0` and every `k >= 1` (rule 3)."""
    if isinstance(request.limit, bool) or not isinstance(request.limit, int) or request.limit < 1:
        raise ValueError(f"limit must be an int >= 1, got {request.limit!r}")
    if (
        isinstance(request.offset, bool)
        or not isinstance(request.offset, int)
        or request.offset < 0
    ):
        raise ValueError(f"offset must be an int >= 0, got {request.offset!r}")
    for retriever in request.retrievers:
        _check_k(retriever)


# ---------------------------------------------------------------- constructors


def match_all() -> MatchAll:
    return MatchAll()


def match_none() -> MatchNone:
    return MatchNone()


def match(
    field: str,
    text: str,
    *,
    operator: Operator = "or",
    minimum_should_match: str | None = None,
    fuzziness: Fuzziness | None = None,
    analyzer: str | None = None,
) -> Match:
    return Match(field, text, operator, minimum_should_match, fuzziness, analyzer)


def match_phrase(field: str, text: str, *, slop: int = 0) -> MatchPhrase:
    return MatchPhrase(field, text, slop)


def multi_match(
    fields: Sequence[tuple[str, float]],
    text: str,
    *,
    kind: MultiMatchKind = "best_fields",
    operator: Operator = "or",
    tie_breaker: float | None = None,
) -> MultiMatch:
    return MultiMatch(tuple(fields), text, kind, operator, tie_breaker)


def term(field: str, value: FieldValue) -> Term:
    return Term(field, value)


def terms(field: str, values: Sequence[FieldValue]) -> Terms:
    return Terms(field, tuple(values))


def range_(
    field: str,
    *,
    gt: FieldValue | None = None,
    gte: FieldValue | None = None,
    lt: FieldValue | None = None,
    lte: FieldValue | None = None,
) -> Range:
    return Range(field, gt, gte, lt, lte)


def exists(field: str) -> Exists:
    return Exists(field)


def is_null(field: str) -> IsNull:
    return IsNull(field)


def is_empty(field: str) -> IsEmpty:
    return IsEmpty(field)


def values_count(
    field: str,
    *,
    gt: int | None = None,
    gte: int | None = None,
    lt: int | None = None,
    lte: int | None = None,
) -> ValuesCount:
    return ValuesCount(field, gt, gte, lt, lte)


def prefix(field: str, value: str) -> Prefix:
    return Prefix(field, value)


def wildcard(field: str, pattern: str) -> Wildcard:
    return Wildcard(field, pattern)


def fuzzy(field: str, value: str, *, fuzziness: Fuzziness = "auto") -> Fuzzy:
    return Fuzzy(field, value, fuzziness)


def ids(*ids: Id) -> Ids:
    return Ids(ids)


def query_string(
    query: str, *, default_fields: Sequence[str] = (), default_operator: Operator = "or"
) -> QueryString:
    return QueryString(query, tuple(default_fields), default_operator)


def bool_(
    *,
    must: Sequence[Query] = (),
    should: Sequence[Query] = (),
    must_not: Sequence[Query] = (),
    filter: Sequence[Query] = (),
    minimum_should_match: str | None = None,
) -> Bool:
    return Bool(tuple(must), tuple(should), tuple(must_not), tuple(filter), minimum_should_match)


def boost(query: Query, boost: float) -> Boost:
    return Boost(query, boost)


def constant_score(query: Query, score: float) -> ConstantScore:
    return ConstantScore(query, score)


def vector(
    field: str,
    query: VectorLike,
    *,
    k: int,
    exact: bool = False,
    nprobes: int | None = None,
    refine_factor: int | None = None,
    ef: int | None = None,
    oversampling: float | None = None,
    filter: Query | None = None,
) -> VectorRetriever:
    """Nearest neighbours of `query` in the dense vector field `field`."""
    params = AnnParams(exact, nprobes, refine_factor, ef, oversampling)
    return VectorRetriever(field, query, k, params, filter)


def text(query: Query | str, *, k: int, fields: Sequence[str] = ()) -> TextRetriever:
    """Full-text search. A string becomes `QueryString` (no field), `Match` (one) or
    `MultiMatch` (several, each boost 1.0)."""
    if not isinstance(query, str):
        if fields:
            raise ValueError("fields apply only to a text query given as a str")
        return TextRetriever(query, k)
    names = list(fields)
    if not names:
        return TextRetriever(QueryString(query), k)
    if len(names) == 1:
        return TextRetriever(Match(names[0], query), k)
    return TextRetriever(MultiMatch(tuple((f, 1.0) for f in names), query), k)


def fused(*inputs: Retriever, fusion: Fusion, k: int) -> FusedRetriever:
    return FusedRetriever(inputs, fusion, k)


def rescore(input: Retriever, field: str, query: VectorLike, *, k: int) -> RescoreRetriever:
    return RescoreRetriever(input, field, query, k)


def sparse(
    field: str,
    indices: Sequence[int],
    values: Sequence[float],
    *,
    k: int,
    filter: Query | None = None,
    idf_corpus: Query | None = None,
) -> SparseRetriever:
    """Exact sparse-vector search; the `SparseVector` is checked here."""
    return SparseRetriever(field, SparseVector(indices, values), k, filter, idf_corpus)


def rrf(k: int = 60) -> Rrf:
    return Rrf(k)


def dbsf() -> Dbsf:
    return Dbsf()


def weighted_sum(*weights: float) -> WeightedSum:
    return WeightedSum(weights)


def score_sort(order: Order = "desc") -> ScoreSort:
    return ScoreSort(order)


def field_sort(
    field: str, order: Order = "asc", *, missing: Literal["first", "last"] = "last"
) -> FieldSort:
    return FieldSort(field, order, missing)


def pk_sort(order: Order = "asc") -> PkSort:
    return PkSort(order)


# ---------------------------------------------------------------- the builder's shared half

B = TypeVar("B", bound="BuilderBase")


class BuilderBase:
    """The immutable half of `SearchBuilder` and `AsyncSearchBuilder`: every method
    returns a new builder."""

    __slots__ = ("_consistency", "_request")

    _request: SearchRequest
    _consistency: Consistency

    def __init__(self, collection: str) -> None:
        self._request = SearchRequest(collection)
        self._consistency = "strong"

    def _with(self: B, **changes: object) -> B:
        clone = copy.copy(self)
        clone._request = replace(self._request, **changes)  # type: ignore[arg-type]
        return clone

    def retrieve(self: B, *retrievers: Retriever) -> B:
        """Appends retrievers."""
        return self._with(retrievers=(*self._request.retrievers, *retrievers))

    def fuse(self: B, fusion: Fusion) -> B:
        return self._with(fusion=fusion)

    def filter(self: B, query: Query) -> B:
        return self._with(filter=query)

    def sort(self: B, *keys: SortKey) -> B:
        return self._with(sort=keys)

    def offset(self: B, n: int) -> B:
        return self._with(offset=n)

    def limit(self: B, n: int) -> B:
        return self._with(limit=n)

    def search_after(self: B, values: Sequence[Any]) -> B:
        return self._with(search_after=tuple(values))

    def score_threshold(self: B, value: float) -> B:
        return self._with(score_threshold=value)

    def select(self: B, projection: Projection) -> B:
        return self._with(select=projection)

    def aggregations(self: B, aggs: Mapping[str, Any]) -> B:
        return self._with(aggregations=aggs)

    def highlight(self: B, spec: Mapping[str, Any]) -> B:
        return self._with(highlight=spec)

    def group_by(self: B, spec: Mapping[str, Any]) -> B:
        return self._with(group_by=spec)

    def track_total_hits(self: B, value: Literal["none", "exact"] | UpTo) -> B:
        return self._with(track_total_hits=value)

    def consistency(self: B, value: Consistency) -> B:
        clone = copy.copy(self)
        clone._consistency = value
        return clone

    def build(self) -> SearchRequest:
        """The request; two or more retrievers without `fuse()` fuse with `Rrf(60)`."""
        request = self._request
        if request.fusion is None and len(request.retrievers) >= 2:
            request = replace(request, fusion=Rrf(60))
        check_request(request)
        return request
