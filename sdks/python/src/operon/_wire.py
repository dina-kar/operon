"""The one module that knows the native REST API's wire JSON (plan M1.6, rows W1-W13, W15).

Everything here is pure: request builders return a `Request`, parsers take a
`Response`. `Client` and `AsyncClient` share it, so their behaviour cannot drift.
"""

from __future__ import annotations

import base64
import datetime
import json
import math
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeAlias
from urllib.parse import quote

from . import query as q
from ._version import __version__
from .errors import OperonError, error_from_body
from .schema import Field, Kind, Schema, SparseVectorField, Text, Vector
from .token import ConsistencyToken
from .types import (
    CollectionInfo,
    Column,
    Delete,
    Document,
    FetchedRecord,
    FetchResult,
    Hit,
    Id,
    LanceVersion,
    Op,
    PartitionInfo,
    Patch,
    Pin,
    ProduceResult,
    Record,
    ScanColumn,
    ScanFragment,
    ScanPlan,
    SearchResponse,
    SparseVector,
    SqlResult,
    StoredDoc,
    StreamInfo,
    TotalHits,
    Upsert,
    WriteResult,
)

TOKEN_HEADER = "Operon-Consistency-Token"
USER_AGENT = f"operon-client-python/{__version__}"

_U64_LIMIT = 2**64

Json: TypeAlias = Any
"""A decoded JSON value."""


@dataclass(frozen=True, slots=True)
class Request:
    """One API call, before it is sent."""

    method: str
    path: str
    query: Mapping[str, str | int] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)
    json_body: Json = None
    idempotent: bool = True
    extra_timeout: float = 0.0
    """Seconds added to the client's timeout for this request (a long poll's wait)."""


@dataclass(frozen=True, slots=True)
class Response:
    """A successful answer: status, headers (lower-cased names) and the raw body."""

    status: int
    headers: Mapping[str, str]
    content: bytes

    def json(self) -> Json:
        return json.loads(self.content)

    def header(self, name: str) -> str | None:
        return self.headers.get(name.lower())


def encode_body(body: Json) -> bytes:
    """Compact JSON; a non-finite float raises `ValueError` before anything is sent."""
    return json.dumps(body, allow_nan=False, separators=(",", ":")).encode("utf-8")


def seg(name: str) -> str:
    """A path segment: every reserved character percent-encoded."""
    return quote(name, safe="")


# ---------------------------------------------------------------- ids


def encode_id(value: object) -> int | str | dict[str, str]:
    """A document id's JSON form: an int in `0..2**64`, a string, or `{"uuid": …}`."""
    if isinstance(value, bool):
        raise TypeError("a bool is not a document id")
    if isinstance(value, int):
        if 0 <= value < _U64_LIMIT:
            return value
        raise ValueError(f"an integer id must be in 0..2**64, got {value}")
    if isinstance(value, str):
        return value
    if isinstance(value, uuid.UUID):
        return {"uuid": str(value)}
    raise TypeError(f"a document id is an int, str or uuid.UUID, not {type(value).__name__}")


def decode_id(value: object) -> int | str | uuid.UUID:
    """The inverse of `encode_id`."""
    if isinstance(value, bool):
        raise ValueError("a bool is not a document id")
    if isinstance(value, int | str):
        return value
    if isinstance(value, Mapping) and set(value) == {"uuid"} and isinstance(value["uuid"], str):
        return uuid.UUID(value["uuid"])
    raise ValueError(f"not a document id: {value!r}")


# ---------------------------------------------------------------- errors


def retry_after_seconds(headers: Mapping[str, str]) -> int | None:
    """`Retry-After` as an integer number of seconds, if it is one."""
    raw = headers.get("retry-after")
    if raw is None:
        return None
    raw = raw.strip()
    if not raw.isdigit():
        return None
    return int(raw)


def error_from_response(status: int, content: bytes) -> OperonError:
    """Maps a non-2xx answer to its typed error."""
    try:
        body = json.loads(content)
    except (ValueError, UnicodeDecodeError):
        body = None
    if isinstance(body, dict) and isinstance(body.get("error"), str):
        return error_from_body(status, body)
    text = content[:800].decode("utf-8", errors="replace")[:200]
    return OperonError(text, code=f"http_{status}", status=status, body={})


# ---------------------------------------------------------------- bytes


def _to_bytes(value: bytes | str) -> bytes:
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value)
    raise TypeError(f"expected bytes or str, not {type(value).__name__}")


def _b64(value: bytes | str) -> str:
    return base64.b64encode(_to_bytes(value)).decode("ascii")


def _unb64(value: object) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"expected a base64 string, got {value!r}")
    return base64.b64decode(value, validate=True)


# ---------------------------------------------------------------- namespaces (W1)


def create_namespace(name: str) -> Request:
    return Request("POST", "/v1/namespaces", json_body={"name": name})


def parse_created_id(response: Response) -> int:
    return int(response.json()["id"])


# ---------------------------------------------------------------- streams (W2-W5)


def _stream_path(ns: str, stream: str) -> str:
    return f"/v1/namespaces/{seg(ns)}/streams/{seg(stream)}"


def create_stream(
    ns: str,
    name: str,
    partitions: int,
    max_age_ms: int | None,
    max_bytes: int | None,
) -> Request:
    body: dict[str, Any] = {"name": name, "partitions": partitions}
    if max_age_ms is not None or max_bytes is not None:
        retention: dict[str, int] = {}
        if max_age_ms is not None:
            retention["max_age_ms"] = max_age_ms
        if max_bytes is not None:
            retention["max_bytes"] = max_bytes
        body["retention"] = retention
    return Request("POST", f"/v1/namespaces/{seg(ns)}/streams", json_body=body)


def get_stream(ns: str, stream: str) -> Request:
    return Request("GET", _stream_path(ns, stream))


def parse_stream_info(response: Response) -> StreamInfo:
    body = response.json()
    retention = body.get("retention") or {}
    return StreamInfo(
        id=int(body["id"]),
        partitions=[
            PartitionInfo(
                partition=int(p["partition"]),
                log_start_offset=int(p["log_start_offset"]),
                high_watermark=int(p["high_watermark"]),
            )
            for p in body.get("partitions", [])
        ],
        max_age_ms=retention.get("max_age_ms"),
        max_bytes=retention.get("max_bytes"),
    )


def _encode_record(record: Record) -> dict[str, Any]:
    if not isinstance(record, Record):
        raise TypeError(f"expected operon.Record, not {type(record).__name__}")
    out: dict[str, Any] = {}
    if record.key is not None:
        out["key"] = _b64(record.key)
    if record.value is not None:
        out["value"] = _b64(record.value)
    if record.headers:
        headers: list[dict[str, str]] = []
        for key, value in record.headers:
            header: dict[str, str] = {"key": key}
            if value is not None:
                header["value"] = _b64(value)
            headers.append(header)
        out["headers"] = headers
    if record.timestamp_ms is not None:
        out["timestamp_ms"] = record.timestamp_ms
    return out


def produce(ns: str, stream: str, partition: int, records: Sequence[Record]) -> Request:
    """W4. Never retried automatically: a 503 may already have been committed (Ruling 5)."""
    return Request(
        "POST",
        f"{_stream_path(ns, stream)}/partitions/{int(partition)}/records",
        json_body={"records": [_encode_record(r) for r in records]},
        idempotent=False,
    )


def token_from_header(response: Response) -> ConsistencyToken | None:
    raw = response.header(TOKEN_HEADER)
    return ConsistencyToken.parse(raw) if raw is not None else None


def parse_produce(response: Response) -> ProduceResult:
    body = response.json()
    token = token_from_header(response)
    if token is None:
        # The body's offsets are the last written; a token names the next one.
        token = ConsistencyToken(
            tuple(
                sorted(
                    (int(t["stream"]), int(t["partition"]), int(t["offset"]) + 1)
                    for t in body.get("token", [])
                )
            )
        )
    return ProduceResult(
        base_offset=int(body["base_offset"]),
        last_offset=int(body["last_offset"]),
        token=token,
    )


def fetch(
    ns: str,
    stream: str,
    partition: int,
    offset: int,
    max_bytes: int | None,
    max_wait_ms: int | None,
) -> Request:
    query: dict[str, str | int] = {"offset": int(offset)}
    if max_bytes is not None:
        query["max_bytes"] = int(max_bytes)
    if max_wait_ms is not None:
        query["max_wait_ms"] = int(max_wait_ms)
    return Request(
        "GET",
        f"{_stream_path(ns, stream)}/partitions/{int(partition)}/records",
        query=query,
        extra_timeout=(max_wait_ms or 0) / 1000,
    )


def parse_fetch(response: Response) -> FetchResult:
    body = response.json()
    return FetchResult(
        records=[
            FetchedRecord(
                offset=int(r["offset"]),
                key=_unb64(r.get("key")),
                value=_unb64(r.get("value")),
                headers=[(str(h["key"]), _unb64(h.get("value"))) for h in r.get("headers", [])],
                timestamp_ms=int(r["timestamp_ms"]),
            )
            for r in body.get("records", [])
        ],
        next_offset=int(body["next_offset"]),
        high_watermark=int(body["high_watermark"]),
        log_start_offset=int(body["log_start_offset"]),
    )


# ---------------------------------------------------------------- vectors and field values


def encode_vector(name: str, value: object) -> list[float]:
    """A dense vector: numbers only (no bools), all finite, sent as floats (rule 4)."""
    if hasattr(value, "tolist") and not isinstance(value, list | tuple):
        value = value.tolist()
    if isinstance(value, str | bytes) or not isinstance(value, Sequence):
        raise ValueError(
            f"vector {name!r} must be a sequence of numbers, not {type(value).__name__}"
        )
    out: list[float] = []
    for element in value:
        if isinstance(element, bool) or not isinstance(element, int | float):
            raise ValueError(f"vector {name!r} holds a non-number: {element!r}")
        number = float(element)
        if not math.isfinite(number):
            raise ValueError(f"vector {name!r} holds a non-finite value: {element!r}")
        out.append(number)
    return out


def encode_sparse(value: SparseVector) -> dict[str, Json]:
    if not isinstance(value, SparseVector):
        raise TypeError(f"expected operon.SparseVector, not {type(value).__name__}")
    return {"indices": list(value.indices), "values": list(value.values)}


def decode_sparse(value: Json) -> SparseVector:
    return SparseVector(tuple(int(i) for i in value["indices"]), tuple(value["values"]))


def _rfc3339(value: datetime.datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"a naive datetime has no zone; give it a tzinfo: {value!r}")
    text = value.isoformat()
    if value.utcoffset() == datetime.timedelta(0) and text.endswith("+00:00"):
        text = text[: -len("+00:00")] + "Z"
    return text


def encode_field_value(value: object) -> Json:
    """A `FieldValue` (row E10): dates as `{"date": RFC 3339}`, ints up to 2**64 - 1."""
    if isinstance(value, bool | str):
        return value
    if isinstance(value, int):
        if -(2**63) <= value < _U64_LIMIT:
            return value
        raise ValueError(f"an integer field value must be in -2**63..2**64, got {value}")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"a field value must be finite, got {value!r}")
        return value
    if isinstance(value, datetime.datetime):
        return {"date": _rfc3339(value)}
    if isinstance(value, datetime.date):
        return {"date": f"{value.isoformat()}T00:00:00Z"}
    raise TypeError(f"not a field value: {value!r}")


def _optional_value(value: object) -> Json:
    return None if value is None else encode_field_value(value)


# ---------------------------------------------------------------- queries


def encode_query(query: object) -> Json:
    """A `Query` in the wire form (the shapes of the plan's wire contract)."""
    if isinstance(query, q.MatchAll):
        return "match_all"
    if isinstance(query, q.MatchNone):
        return "match_none"
    if isinstance(query, q.Match):
        return {
            "match": {
                "field": query.field,
                "text": query.text,
                "operator": query.operator,
                "minimum_should_match": query.minimum_should_match,
                "fuzziness": query.fuzziness,
                "analyzer": query.analyzer,
            }
        }
    if isinstance(query, q.MatchPhrase):
        return {"match_phrase": {"field": query.field, "text": query.text, "slop": query.slop}}
    if isinstance(query, q.MultiMatch):
        return {
            "multi_match": {
                "fields": [[name, float(weight)] for name, weight in query.fields],
                "text": query.text,
                "kind": query.kind,
                "operator": query.operator,
                "tie_breaker": query.tie_breaker,
            }
        }
    if isinstance(query, q.Term):
        return {"term": {"field": query.field, "value": encode_field_value(query.value)}}
    if isinstance(query, q.Terms):
        return {
            "terms": {"field": query.field, "values": [encode_field_value(v) for v in query.values]}
        }
    if isinstance(query, q.Range):
        return {
            "range": {
                "field": query.field,
                "gt": _optional_value(query.gt),
                "gte": _optional_value(query.gte),
                "lt": _optional_value(query.lt),
                "lte": _optional_value(query.lte),
            }
        }
    if isinstance(query, q.Exists):
        return {"exists": {"field": query.field}}
    if isinstance(query, q.IsNull):
        return {"is_null": {"field": query.field}}
    if isinstance(query, q.IsEmpty):
        return {"is_empty": {"field": query.field}}
    if isinstance(query, q.ValuesCount):
        return {
            "values_count": {
                "field": query.field,
                "gt": query.gt,
                "gte": query.gte,
                "lt": query.lt,
                "lte": query.lte,
            }
        }
    if isinstance(query, q.Prefix):
        return {"prefix": {"field": query.field, "value": query.value}}
    if isinstance(query, q.Wildcard):
        return {"wildcard": {"field": query.field, "pattern": query.pattern}}
    if isinstance(query, q.Fuzzy):
        return {"fuzzy": {"field": query.field, "value": query.value, "fuzziness": query.fuzziness}}
    if isinstance(query, q.Ids):
        return {"ids": [encode_id(i) for i in query.ids]}
    if isinstance(query, q.QueryString):
        return {
            "query_string": {
                "query": query.query,
                "default_fields": list(query.default_fields),
                "default_operator": query.default_operator,
            }
        }
    if isinstance(query, q.Bool):
        return {
            "bool": {
                "must": [encode_query(x) for x in query.must],
                "should": [encode_query(x) for x in query.should],
                "must_not": [encode_query(x) for x in query.must_not],
                "filter": [encode_query(x) for x in query.filter],
                "minimum_should_match": query.minimum_should_match,
            }
        }
    if isinstance(query, q.Boost):
        return {"boost": {"query": encode_query(query.query), "boost": float(query.boost)}}
    if isinstance(query, q.ConstantScore):
        return {"constant_score": {"query": encode_query(query.query), "score": float(query.score)}}
    raise TypeError(f"not a query: {query!r}")


def _optional_query(query: object) -> Json:
    return None if query is None else encode_query(query)


def encode_fusion(fusion: object) -> Json:
    if isinstance(fusion, q.Rrf):
        return {"rrf": {"k": fusion.k}}
    if isinstance(fusion, q.Dbsf):
        return "dbsf"
    if isinstance(fusion, q.WeightedSum):
        return {"weighted_sum": {"weights": [float(w) for w in fusion.weights]}}
    raise TypeError(f"not a fusion: {fusion!r}")


def _encode_params(params: q.AnnParams) -> dict[str, Json]:
    return {
        "exact": params.exact,
        "nprobes": params.nprobes,
        "refine_factor": params.refine_factor,
        "ef": params.ef,
        "oversampling": params.oversampling,
        "distance": None,
    }


def encode_retriever(retriever: object) -> Json:
    if isinstance(retriever, q.VectorRetriever):
        return {
            "vector": {
                "field": retriever.field,
                "query": encode_vector(retriever.field, retriever.query),
                "k": retriever.k,
                "params": _encode_params(retriever.params),
                "filter": _optional_query(retriever.filter),
            }
        }
    if isinstance(retriever, q.TextRetriever):
        return {"text": {"query": encode_query(retriever.query), "k": retriever.k}}
    if isinstance(retriever, q.FusedRetriever):
        return {
            "fused": {
                "inputs": [encode_retriever(r) for r in retriever.inputs],
                "fusion": encode_fusion(retriever.fusion),
                "k": retriever.k,
            }
        }
    if isinstance(retriever, q.RescoreRetriever):
        return {
            "rescore": {
                "input": encode_retriever(retriever.input),
                "field": retriever.field,
                "query": encode_vector(retriever.field, retriever.query),
                "k": retriever.k,
            }
        }
    if isinstance(retriever, q.SparseRetriever):
        return {
            "sparse": {
                "field": retriever.field,
                "query": encode_sparse(retriever.query),
                "k": retriever.k,
                "filter": _optional_query(retriever.filter),
                "params": {"idf_corpus": _optional_query(retriever.idf_corpus)},
            }
        }
    raise TypeError(f"not a retriever: {retriever!r}")


def encode_sort_key(key: object) -> Json:
    if isinstance(key, q.ScoreSort):
        return {"score": {"order": key.order}}
    if isinstance(key, q.FieldSort):
        return {"field": {"field": key.field, "order": key.order, "missing": key.missing}}
    if isinstance(key, q.PkSort):
        return {"pk": {"order": key.order}}
    raise TypeError(f"not a sort key: {key!r}")


def encode_projection(projection: q.Projection) -> dict[str, Json]:
    source: Json
    if isinstance(projection.source, q.SourcePaths):
        source = {
            "include": list(projection.source.include),
            "exclude": list(projection.source.exclude),
        }
    elif projection.source in ("all", "none"):
        source = projection.source
    else:
        raise ValueError(
            f"a projection's source is 'all', 'none' or SourcePaths: {projection.source!r}"
        )
    return {
        "source": source,
        "vectors": list(projection.vectors),
        "fields": list(projection.fields),
    }


def _sort_value(value: object) -> Json:
    return {"uuid": str(value)} if isinstance(value, uuid.UUID) else value


def _track_total_hits(value: object) -> Json:
    if isinstance(value, q.UpTo):
        return {"up_to": value.n}
    if value in ("none", "exact"):
        return value
    raise ValueError(f"track_total_hits is 'none', 'exact' or UpTo: {value!r}")


# ---------------------------------------------------------------- consistency (Ruling 6)


@dataclass(frozen=True, slots=True)
class ReadConsistency:
    """How one read sends its consistency: a body value and/or the token header."""

    body: Json = None
    """`"eventual"` or a pinned object, for W11 and W13 bodies; `None` sends no key."""
    search_body: Json = "strong"
    """The SearchRequest's `consistency` (always sent)."""
    header: str | None = None


def read_consistency(value: object) -> ReadConsistency:
    if isinstance(value, Pin):
        pinned = {"pinned": {"manifest_version": value.manifest_version, "token": str(value.token)}}
        return ReadConsistency(body=pinned, search_body=pinned)
    if value == "strong":
        return ReadConsistency()
    if value == "eventual":
        return ReadConsistency(body="eventual", search_body="eventual")
    if isinstance(value, str):
        value = ConsistencyToken.parse(value)
    if isinstance(value, ConsistencyToken):
        text = str(value)
        return ReadConsistency(search_body={"at_least": text}, header=text)
    raise TypeError(f"not a consistency: {value!r}")


def _token_headers(consistency: ReadConsistency) -> dict[str, str]:
    return {TOKEN_HEADER: consistency.header} if consistency.header is not None else {}


# ---------------------------------------------------------------- schemas


def _encode_kind(kind: object) -> Json:
    if isinstance(kind, Text):
        return {"text": {"analyzer": kind.analyzer, "positions": kind.positions}}
    if isinstance(kind, str):
        return kind
    raise TypeError(f"not a field kind: {kind!r}")


def encode_schema(schema: Schema) -> dict[str, Json]:
    return {
        "fields": [
            {
                "name": f.name,
                "source_path": f.source_path if f.source_path is not None else f.name,
                "kind": _encode_kind(f.kind),
                "indexed": f.indexed,
                "fast": f.fast,
            }
            for f in schema.fields
        ],
        "vectors": [{"name": v.name, "dim": v.dim, "distance": v.distance} for v in schema.vectors],
        "sparse_vectors": [{"name": s.name, "modifier": s.modifier} for s in schema.sparse_vectors],
        "dynamic": schema.dynamic,
        "max_fields": schema.max_fields,
    }


def _decode_kind(value: Json) -> Kind:
    if value == "text":
        return Text()
    if isinstance(value, dict) and "text" in value:
        spec = value["text"] or {}
        return Text(spec.get("analyzer", "standard"), bool(spec.get("positions", True)))
    if isinstance(value, str):
        return value  # type: ignore[return-value]
    raise ValueError(f"unknown field kind: {value!r}")


def decode_schema(value: Json) -> Schema:
    return Schema(
        fields=tuple(
            Field(
                name=f["name"],
                kind=_decode_kind(f["kind"]),
                source_path=f.get("source_path"),
                indexed=bool(f.get("indexed", True)),
                fast=bool(f.get("fast", False)),
            )
            for f in value.get("fields", [])
        ),
        vectors=tuple(
            Vector(v["name"], int(v["dim"]), v.get("distance") or "cosine")
            for v in value.get("vectors", [])
        ),
        sparse_vectors=tuple(
            SparseVectorField(s["name"], s.get("modifier", "none"))
            for s in value.get("sparse_vectors", [])
        ),
        dynamic=value.get("dynamic", "strict"),
        max_fields=int(value.get("max_fields", 1000)),
        version=value.get("version"),
    )


# ---------------------------------------------------------------- collections (W6-W9)


def _collections_path(ns: str) -> str:
    return f"/v1/namespaces/{seg(ns)}/collections"


def _collection_path(ns: str, collection: str) -> str:
    return f"{_collections_path(ns)}/{seg(collection)}"


def create_collection(ns: str, name: str, schema: Schema, partitions: int | None) -> Request:
    body: dict[str, Json] = {"name": name, "schema": encode_schema(schema)}
    if partitions is not None:
        body["partitions"] = partitions
    return Request("POST", _collections_path(ns), json_body=body)


def get_collection(ns: str, name: str) -> Request:
    return Request("GET", _collection_path(ns, name))


def list_collections(ns: str) -> Request:
    return Request("GET", _collections_path(ns))


def drop_collection(ns: str, name: str) -> Request:
    return Request("DELETE", _collection_path(ns, name))


def _optional_int(value: Json) -> int | None:
    return None if value is None else int(value)


def decode_collection_info(value: Json) -> CollectionInfo:
    return CollectionInfo(
        id=int(value["id"]),
        name=str(value["name"]),
        schema=decode_schema(value["schema"]),
        partitions=_optional_int(value.get("partitions")),
        live_doc_count=_optional_int(value.get("live_doc_count")),
        raw=value,
    )


def parse_collection_info(response: Response) -> CollectionInfo:
    return decode_collection_info(response.json())


def parse_collection_list(response: Response) -> list[CollectionInfo]:
    return [decode_collection_info(c) for c in response.json().get("collections", [])]


def parse_dropped(response: Response) -> bool:
    return bool(response.json()["dropped"])


# ---------------------------------------------------------------- documents (W10, W11)


def _encode_vectors(vectors: Mapping[str, object]) -> dict[str, Json]:
    return {name: encode_vector(name, v) for name, v in vectors.items()}


def _encode_sparse_vectors(vectors: Mapping[str, SparseVector]) -> dict[str, Json]:
    return {name: encode_sparse(v) for name, v in vectors.items()}


def encode_document(doc: Document) -> dict[str, Json]:
    if not isinstance(doc, Document):
        raise TypeError(f"expected operon.Document, not {type(doc).__name__}")
    return {
        "id": encode_id(doc.id),
        "source": dict(doc.source),
        "vectors": _encode_vectors(doc.vectors),
        "sparse_vectors": _encode_sparse_vectors(doc.sparse_vectors),
    }


def encode_op(op: object) -> dict[str, Json]:
    if isinstance(op, Upsert):
        return {"upsert": encode_document(op.doc)}
    if isinstance(op, Delete):
        return {"delete": {"id": encode_id(op.id)}}
    if isinstance(op, Patch):
        return {
            "patch": {
                "id": encode_id(op.id),
                "mode": op.mode,
                "source": dict(op.source),
                "delete_keys": list(op.delete_keys),
                "vectors": {
                    name: None if v is None else encode_vector(name, v)
                    for name, v in op.vectors.items()
                },
                "sparse_vectors": {
                    name: None if v is None else encode_sparse(v)
                    for name, v in op.sparse_vectors.items()
                },
                "upsert": None if op.upsert is None else encode_document(op.upsert),
            }
        }
    raise TypeError(f"not a write op: {op!r}")


def write(ns: str, collection: str, ops: Sequence[Op], report_existence: bool) -> Request:
    """W10: one request, atomic across partitions; a keyed write, so retried (Ruling 5)."""
    ops = list(ops)
    if not ops:
        raise ValueError("an empty write: give at least one op")
    return Request(
        "POST",
        f"{_collection_path(ns, collection)}/documents",
        json_body={"ops": [encode_op(op) for op in ops], "report_existence": report_existence},
        idempotent=True,
    )


def _read_token(response: Response, body: Json, key: str) -> ConsistencyToken:
    token = token_from_header(response)
    if token is not None:
        return token
    return ConsistencyToken.parse(body[key])


def parse_write(response: Response) -> WriteResult:
    body = response.json()
    return WriteResult(
        token=_read_token(response, body, "token"),
        results=[str(r) for r in body.get("results", [])],
    )


def get_documents(
    ns: str, collection: str, ids: Sequence[Id], select: q.Projection | None, consistency: object
) -> Request:
    read = read_consistency(consistency)
    body: dict[str, Json] = {
        "ids": [encode_id(i) for i in ids],
        "select": encode_projection(select if select is not None else q.Projection()),
    }
    if read.body is not None:
        body["consistency"] = read.body
    return Request(
        "POST",
        f"{_collection_path(ns, collection)}/documents/get",
        headers=_token_headers(read),
        json_body=body,
    )


def _decode_vectors(value: Json) -> dict[str, list[float]]:
    return {name: [float(x) for x in v] for name, v in (value or {}).items()}


def _decode_sparse_vectors(value: Json) -> dict[str, SparseVector]:
    return {name: decode_sparse(v) for name, v in (value or {}).items()}


def decode_stored_doc(value: Json) -> StoredDoc | None:
    if value is None:
        return None
    return StoredDoc(
        id=decode_id(value["id"]),
        source=value.get("source") or {},
        vectors=_decode_vectors(value.get("vectors")),
        sparse_vectors=_decode_sparse_vectors(value.get("sparse_vectors")),
    )


def parse_documents(response: Response) -> list[StoredDoc | None]:
    return [decode_stored_doc(d) for d in response.json()["documents"]]


# ---------------------------------------------------------------- search (W12)


def encode_search_request(request: q.SearchRequest, consistency: object) -> dict[str, Json]:
    q.check_request(request)
    read = read_consistency(consistency)
    return {
        "collection": request.collection,
        "consistency": read.search_body,
        "retrievers": [encode_retriever(r) for r in request.retrievers],
        "fusion": None if request.fusion is None else encode_fusion(request.fusion),
        "filter": _optional_query(request.filter),
        "sort": [encode_sort_key(k) for k in request.sort],
        "offset": request.offset,
        "limit": request.limit,
        "search_after": (
            None if request.search_after is None else [_sort_value(v) for v in request.search_after]
        ),
        "score_threshold": request.score_threshold,
        "select": encode_projection(request.select),
        "aggregations": None if request.aggregations is None else dict(request.aggregations),
        "highlight": None if request.highlight is None else dict(request.highlight),
        "group_by": None if request.group_by is None else dict(request.group_by),
        "track_total_hits": _track_total_hits(request.track_total_hits),
    }


def query(ns: str, request: q.SearchRequest, consistency: object) -> Request:
    body = encode_search_request(request, consistency)
    return Request(
        "POST",
        f"/v1/namespaces/{seg(ns)}/query",
        headers=_token_headers(read_consistency(consistency)),
        json_body=body,
    )


def decode_hit(value: Json) -> Hit:
    return Hit(
        id=decode_id(value["pk"]),
        score=float(value["score"]),
        sort_values=list(value.get("sort_values", [])),
        source=value.get("source"),
        vectors=_decode_vectors(value.get("vectors")),
        sparse_vectors=_decode_sparse_vectors(value.get("sparse_vectors")),
        highlight={k: list(v) for k, v in (value.get("highlight") or {}).items()},
    )


def parse_search(response: Response) -> SearchResponse:
    body = response.json()
    total = body.get("total")
    return SearchResponse(
        hits=[decode_hit(h) for h in body.get("hits", [])],
        total=None if total is None else TotalHits(int(total["value"]), total["relation"]),
        aggregations=body.get("aggregations"),
        groups=body.get("groups"),
        read_token=_read_token(response, body, "read_token"),
    )


# ---------------------------------------------------------------- SQL (W13)


def sql(ns: str, query_text: str, consistency: object) -> Request:
    read = read_consistency(consistency)
    body: dict[str, Json] = {"query": query_text}
    if read.body is not None:
        body["consistency"] = read.body
    return Request(
        "POST", f"/v1/namespaces/{seg(ns)}/sql", headers=_token_headers(read), json_body=body
    )


def parse_sql(response: Response) -> SqlResult:
    body = response.json()
    return SqlResult(
        columns=[Column(str(c["name"]), str(c["type"])) for c in body.get("columns", [])],
        rows=[list(r) for r in body.get("rows", [])],
        truncated=bool(body.get("truncated", False)),
    )


# ---------------------------------------------------------------- scan plans (W15)


def encode_scan_at(at: object) -> Json:
    if isinstance(at, bool):
        raise TypeError("a bool is not a scan point")
    if at == "current":
        return "current"
    if isinstance(at, int):
        if not 0 <= at < _U64_LIMIT:
            raise ValueError(f"a manifest version must be in 0..2**64, got {at}")
        return {"manifest_version": at}
    if isinstance(at, str):
        at = ConsistencyToken.parse(at)
    if isinstance(at, ConsistencyToken):
        return {"token": str(at)}
    raise TypeError(f"a scan point is 'current', an int or a token, not {at!r}")


def scan_plan(ns: str, collection: str, at: object) -> Request:
    return Request(
        "POST",
        f"{_collection_path(ns, collection)}/scan",
        json_body={"at": encode_scan_at(at)},
    )


def _decode_pin(value: Json) -> Pin:
    return Pin(int(value["manifest_version"]), ConsistencyToken.parse(value["token"]))


def decode_scan_plan(value: Json) -> ScanPlan:
    lance = value.get("lance")
    return ScanPlan(
        collection=str(value["collection"]),
        collection_id=int(value["collection_id"]),
        manifest_version=int(value["manifest_version"]),
        lance=(
            None
            if lance is None
            else LanceVersion(lance.get("uri"), int(lance["version"]), str(lance["manifest_path"]))
        ),
        fragments=[
            ScanFragment(
                id=int(f["id"]),
                physical_rows=int(f["physical_rows"]),
                deleted_rows=int(f["deleted_rows"]),
                live_rows=int(f["live_rows"]),
                files=[str(file["path"]) for file in f.get("files", [])],
                deletion_file=(
                    None if f.get("deletion_file") is None else str(f["deletion_file"]["path"])
                ),
                lance=f.get("lance") or {},
            )
            for f in value.get("fragments", [])
        ],
        live_rows=int(value["live_rows"]),
        columns=[
            ScanColumn(
                name=str(c["name"]),
                data_type=str(c["data_type"]),
                role=str(c["role"]),
                vector=c.get("vector"),
                dim=_optional_int(c.get("dim")),
            )
            for c in value.get("columns", [])
        ],
        tail=bool(value["tail"]),
        tail_records=int(value.get("tail_records", 0)),
        durable_token=ConsistencyToken.parse(value["durable_token"]),
        pin=_decode_pin(value["pin"]),
        planned_at_ms=int(value.get("planned_at_ms", 0)),
        expires_at_ms=_optional_int(value.get("expires_at_ms")),
        raw=value,
    )


def parse_scan_plan(response: Response) -> ScanPlan:
    return decode_scan_plan(response.json())
