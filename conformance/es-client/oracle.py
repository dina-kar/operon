"""The ES 8.19 oracle probes (plan M1.5 Task 11, owner ruling O-M15-6).

Each probe runs the same requests against Loams and against a real
Elasticsearch 8.19, used only as a test oracle (nothing from it is vendored
or shipped, R14), and compares what the plan marks "verify": the status and
the error's `type` and `reason` (with its root cause and `caused_by`), or a
normalised body. `test_oracle.py` runs every probe as a test when
`ES_ORACLE_URL` is set; `python oracle.py <loams-url> <oracle-url>` prints
a report.

Raw HTTP, not the client: the texts, statuses and headers are the subject.
"""

from __future__ import annotations

import json
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

JSON = "application/json"
NDJSON = "application/x-ndjson"


@dataclass
class Answer:
    status: int
    body: Any
    headers: dict


def send(base, method, path, body=None, content_type=JSON, raw=None):
    """One request; `raw` bytes go as they are (with `content_type`, if any)."""
    data = None
    headers = {}
    if raw is not None:
        data = raw
        if content_type:
            headers["Content-Type"] = content_type
    elif body is not None:
        if content_type == NDJSON:
            data = "".join(json.dumps(line) + "\n" for line in body).encode()
        else:
            data = json.dumps(body).encode()
        headers["Content-Type"] = content_type
    request = urllib.request.Request(base + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            status, text, hdrs = response.status, response.read().decode(), dict(response.headers)
    except urllib.error.HTTPError as err:
        status, text, hdrs = err.code, err.read().decode(), dict(err.headers)
    try:
        parsed = json.loads(text) if text else None
    except ValueError:
        parsed = text
    return Answer(status, parsed, {k.lower(): v for k, v in hdrs.items()})


POSITION = re.compile(r"^\[\d+:\d+\] ")


def _norm_text(text):
    """A reason without its leading `[line:col] `: Loams reports `[1:1]`
    where ES reports the value's position (row T4-5, a kept deviation)."""
    return POSITION.sub("", text) if isinstance(text, str) else text


def error_view(answer: Answer):
    """The status and the error's type and reason, its first root cause and
    its `caused_by` chain."""
    body = answer.body
    if not isinstance(body, dict) or "error" not in body:
        return {"status": answer.status}
    err = body["error"]
    if isinstance(err, str):
        return {"status": answer.status, "error": err}
    view = {"status": answer.status, "type": err.get("type"), "reason": _norm_text(err.get("reason"))}
    roots = err.get("root_cause") or []
    if roots:
        view["root"] = (roots[0].get("type"), _norm_text(roots[0].get("reason")))
    cause = err.get("caused_by")
    chain = []
    while isinstance(cause, dict):
        chain.append((cause.get("type"), _norm_text(cause.get("reason"))))
        cause = cause.get("caused_by")
    if chain:
        view["caused_by"] = chain
    return view


VOLATILE = {"took", "index_uuid", "uuid", "creation_date", "_seq_no", "_version", "_primary_term", "_shards", "node", "_node", "version", "provided_name"}


def strip(value):
    """`value` without volatile keys (timings, uuids, versions)."""
    if isinstance(value, dict):
        return {k: strip(v) for k, v in value.items() if k not in VOLATILE}
    if isinstance(value, list):
        return [strip(v) for v in value]
    return value


@dataclass
class Probe:
    """`setup` requests (method, path, body[, content type]) then `request`;
    `view` turns the answer into what is compared."""

    id: str
    item: str
    request: tuple
    setup: list = field(default_factory=list)
    view: Callable[[Answer], Any] = error_view
    indices: tuple = ()
    # A recorded deviation: the plan row that keeps Loams's answer.
    deviation: Optional[str] = None


ALWAYS_CLEAN = ("o_missing", "o_nope")


def run_probe(base, probe: Probe):
    for name in probe.indices + ALWAYS_CLEAN:
        send(base, "DELETE", f"/{name}")
    for step in probe.setup:
        method, path, *rest = step
        body = rest[0] if rest else None
        ctype = rest[1] if len(rest) > 1 else (NDJSON if path.endswith("_bulk") or "_bulk?" in path else JSON)
        send(base, method, path, body, ctype)
    method, path, *rest = probe.request
    body = rest[0] if rest else None
    kwargs = rest[1] if len(rest) > 1 else {}
    answer = send(base, method, path, body, **kwargs)
    try:
        return probe.view(answer)
    finally:
        for name in probe.indices + ALWAYS_CLEAN:
            send(base, "DELETE", f"/{name}")


def unordered_view(answer):
    """`error_view` with the order of listed names ignored: ES lists some
    (write indices, unsupported root parameters) in hash order."""
    view = error_view(answer)

    def norm(text):
        if not isinstance(text, str):
            return text
        groups = re.findall(r"\[[^\[\]]*\]", text)
        return sorted(g.strip("[]").replace(" ", "") for g in groups for g in g.split(","))

    return {k: (norm(v) if k == "reason" else v) for k, v in view.items() if k in ("status", "type", "reason")}


def body_view(answer):
    return {"status": answer.status, "body": strip(answer.body)}


def keys_view(answer):
    body = answer.body if isinstance(answer.body, dict) else {}
    return {"status": answer.status, "keys": list(body)}


def count_view(answer):
    body = answer.body if isinstance(answer.body, dict) else {}
    return {"status": answer.status, "count": body.get("count"), "error": error_view(answer).get("reason")}


def hits_view(answer):
    body = answer.body if isinstance(answer.body, dict) else {}
    hits = body.get("hits", {}).get("hits", [])
    return {"status": answer.status, "ids": sorted(h["_id"] for h in hits), **{k: v for k, v in error_view(answer).items() if k != "status"}}


def source_view(answer):
    body = answer.body if isinstance(answer.body, dict) else {}
    return {"status": answer.status, "_source": body.get("_source", body if "found" not in body else None)}


def mget_view(answer):
    body = answer.body if isinstance(answer.body, dict) else {}
    if "docs" not in body:
        return error_view(answer)
    docs = []
    for d in body["docs"]:
        entry = {k: d.get(k) for k in ("_index", "_id", "found")}
        if "error" in d:
            e = d["error"]
            entry["error"] = (e.get("type"), e.get("reason")) if isinstance(e, dict) else e
        docs.append(entry)
    return {"status": answer.status, "docs": docs}


def bulk_view(answer):
    body = answer.body if isinstance(answer.body, dict) else {}
    if "items" not in body:
        return error_view(answer)
    items = []
    for item in body["items"]:
        ((action, result),) = item.items()
        e = result.get("error")
        items.append((action, result.get("status"), (e.get("type"), e.get("reason")) if isinstance(e, dict) else None))
    return {"status": answer.status, "errors": body.get("errors"), "items": items}


def msearch_view(answer):
    body = answer.body if isinstance(answer.body, dict) else {}
    if "responses" not in body:
        return error_view(answer)
    out = []
    for r in body["responses"]:
        if "error" in r:
            e = r["error"]
            out.append((r.get("status"), e.get("type"), e.get("reason")))
        else:
            out.append((r.get("status"), len(r["hits"]["hits"])))
    return {"status": answer.status, "responses": out}


def status_view(answer):
    return {"status": answer.status}


def allow_view(answer):
    view = error_view(answer)
    return view


def index_list(answer):
    body = answer.body if isinstance(answer.body, dict) else {}
    return {"status": answer.status, "indices": sorted(k for k in body if k.startswith("o_")) if answer.status == 200 else None, **{k: v for k, v in error_view(answer).items() if k != "status"}}


# Fixtures shared by the probes.
def mk(name, body=None):
    return ("PUT", f"/{name}", body)


def doc(index, id_, body):
    return ("PUT", f"/{index}/_doc/{id_}?refresh=true", body)


TWO = [mk("o_1"), mk("o_2")]
NO_WRITE_ALIAS = TWO + [("POST", "/_aliases", {"actions": [{"add": {"index": "o_1", "alias": "o_a"}}, {"add": {"index": "o_2", "alias": "o_a"}}]})]
CLEAN = ("o_1", "o_2", "o_3", "o_a")

PROBES: list[Probe] = [
    # --- Ruling 9 ---
    Probe("r9_no_write_index", "Ruling 9 no-write-index text", ("PUT", "/o_a/_doc/1", {"a": 1}), NO_WRITE_ALIAS, indices=CLEAN),
    Probe("r9_bulk_no_write_index", "Ruling 9 no-write-index text (bulk item)", ("POST", "/_bulk", [{"index": {"_index": "o_a", "_id": "1"}}, {"a": 1}], {"content_type": NDJSON}), NO_WRITE_ALIAS, view=bulk_view, indices=CLEAN),
    Probe("r9_multi_member_get", "Ruling 9 multi-member single-document read", ("GET", "/o_a/_doc/1"), NO_WRITE_ALIAS, indices=CLEAN),
    Probe("r9_multi_member_source", "Ruling 9 multi-member _source read", ("GET", "/o_a/_source/1"), NO_WRITE_ALIAS, indices=CLEAN),
    Probe("r9_multi_member_mget", "Ruling 9 multi-member _mget entry", ("POST", "/_mget", {"docs": [{"_index": "o_a", "_id": "1"}]}), NO_WRITE_ALIAS, view=mget_view, indices=CLEAN),
    Probe(
        "r9_readd_unsets_write",
        "Ruling 9 add of an existing pair without is_write_index unsets it",
        ("GET", "/_alias/o_a"),
        TWO + [
            ("POST", "/_aliases", {"actions": [{"add": {"index": "o_1", "alias": "o_a", "is_write_index": True}}]}),
            ("POST", "/_aliases", {"actions": [{"add": {"index": "o_1", "alias": "o_a"}}]}),
        ],
        view=body_view,
        indices=CLEAN,
    ),
    # --- T1-5 ---
    Probe("t1_5_write_comma_list", "T1-5 write to a comma list", ("PUT", "/o_1,o_2/_doc/1", {"a": 1}), TWO, indices=CLEAN),
    Probe("t1_5_write_wildcard", "T1-5 write to a wildcard", ("PUT", "/o_*/_doc/1", {"a": 1}), TWO, indices=CLEAN),
    Probe("t1_5_bulk_comma_list", "T1-5 bulk item to a comma list", ("POST", "/_bulk", [{"index": {"_index": "o_1,o_2", "_id": "1"}}, {"a": 1}], {"content_type": NDJSON}), TWO, view=bulk_view, indices=CLEAN),
    # --- T1-7 ---
    Probe("t1_7_missing_content_type", "T1-7 body without Content-Type", ("POST", "/o_1/_search", None, {"raw": b'{"query":{"match_all":{}}}', "content_type": None}), TWO, indices=CLEAN),
    Probe("t1_7_unknown_params", "T1-7 several unknown parameters", ("GET", "/o_1/_search?zz=1&aa=2"), TWO, indices=CLEAN),
    Probe("t1_7_unknown_param", "T1-7 one unknown parameter", ("GET", "/o_1/_search?zz=1"), TWO, indices=CLEAN),
    Probe("t1_7_unknown_route", "T1-7 unknown route keeps the query string", ("GET", "/o_1/_doc/1/zzz?y=1"), view=body_view),
    Probe("t1_7_405_index", "T1-7 405 on an index (HEAD registered)", ("PATCH", "/o_1"), TWO, view=body_view, indices=CLEAN),
    Probe("t1_7_405_doc", "T1-7 405 on a document", ("PATCH", "/o_1/_doc/1"), TWO, view=body_view, indices=CLEAN),
    Probe("t1_7_405_mapping", "T1-7 405 on _mapping", ("DELETE", "/o_1/_mapping"), TWO, view=body_view, indices=CLEAN),
    Probe("t1_7_method_not_allowed", "T1-7 405 method list", ("POST", "/_cluster/health")),
    Probe("t1_7_method_not_allowed_index", "T1-7 405 on an index route", ("DELETE", "/o_1/_search"), TWO, indices=CLEAN),
    # --- O-M15-7 ---
    Probe("o_m15_7_health_missing_body", "O-M15-7 _cluster/health of a missing index (body)", ("GET", "/_cluster/health/o_missing?timeout=1s"), view=lambda a: {"status": a.status, "body": {k: v for k, v in (a.body or {}).items() if k not in ("cluster_name",)}}),
    Probe("o_m15_7_health_missing", "O-M15-7 _cluster/health of a missing index", ("GET", "/_cluster/health/o_missing?timeout=1s"), view=lambda a: {"status": a.status, "health": a.body.get("status") if isinstance(a.body, dict) else None, "timed_out": a.body.get("timed_out") if isinstance(a.body, dict) else None, **{k: v for k, v in error_view(a).items() if k != "status"}}),
    # --- Task 2 texts (T2-5, T2-6, T2-7) ---
    Probe("t2_5_unknown_mapper_param", "T2-5 unknown parameter on mapper", mk("o_3", {"mappings": {"properties": {"f": {"type": "keyword", "foo": 1}}}}), indices=CLEAN),
    Probe("t2_5_no_handler", "T2-5 No handler for type", mk("o_3", {"mappings": {"properties": {"f": {"type": "nosuch"}}}}), indices=CLEAN),
    Probe("t2_5_root_unsupported", "T2-5 Root mapping unsupported parameters", mk("o_3", {"mappings": {"zzz": 1, "properties": {}}}), indices=CLEAN),
    Probe("t2_5_root_unsupported_two", "T2-5 Root mapping, two unsupported parameters", mk("o_3", {"mappings": {"zzz": 1, "yyy": "v", "properties": {}}}), indices=CLEAN, view=unordered_view),
    Probe("t2_5_dims_too_big", "T2-5 dims range", mk("o_3", {"mappings": {"properties": {"v": {"type": "dense_vector", "dims": 5000}}}}), indices=CLEAN),
    Probe("t2_5_dims_zero", "T2-5 dims range (0)", mk("o_3", {"mappings": {"properties": {"v": {"type": "dense_vector", "dims": 0}}}}), indices=CLEAN),
    Probe("t2_5_unknown_similarity", "T2-5 Unknown vector similarity", mk("o_3", {"mappings": {"properties": {"v": {"type": "dense_vector", "dims": 3, "similarity": "foo"}}}}), indices=CLEAN),
    Probe("t2_5_unknown_setting", "T2-5 unknown setting", mk("o_3", {"settings": {"index.foo": 1}}), indices=CLEAN),
    Probe("t2_5_setting_range", "T2-5 numeric setting range", mk("o_3", {"settings": {"number_of_shards": 0}}), indices=CLEAN),
    Probe("t2_5_similarity_without_type", "T2-5 similarity without a type", mk("o_3", {"settings": {"index.similarity.my": {"b": 0.5}}}), indices=CLEAN),
    Probe("t2_5_total_fields_limit", "T2-5 field limit at creation", mk("o_3", {"settings": {"index.mapping.total_fields.limit": 2}, "mappings": {"properties": {"a": {"type": "keyword"}, "b": {"type": "keyword"}, "c": {"type": "keyword"}}}}), indices=CLEAN),
    Probe("t2_5_total_fields_dynamic", "T2-5 field limit on a dynamic write", ("PUT", "/o_3/_doc/1", {"a": "x", "b": "y", "c": 1}), [mk("o_3", {"settings": {"index.mapping.total_fields.limit": 2}})], indices=CLEAN, deviation='T11-5: ES counts the new fields at the point parsing crossed the limit'),
    Probe("t2_6_strict_dynamic", "T2-6 strict dynamic text", ("PUT", "/o_3/_doc/1", {"known": "x", "zzz": 1}), [mk("o_3", {"mappings": {"dynamic": "strict", "properties": {"known": {"type": "keyword"}}}})], indices=CLEAN),
    Probe("t2_6_strict_dynamic_nested", "T2-6 strict dynamic text inside an object", ("PUT", "/o_3/_doc/1", {"a": {"b": 1}}), [mk("o_3", {"mappings": {"dynamic": "strict", "properties": {"a": {"type": "object"}}}})], indices=CLEAN),
    Probe("t2_5_put_mapping_unknown_param", "T2-5 unknown parameter through PUT _mapping", ("PUT", "/o_1/_mapping", {"properties": {"f": {"type": "keyword", "foo": 1}}}), TWO, indices=CLEAN),
    Probe("t2_5_put_mapping_no_handler", "T2-5 unknown type through PUT _mapping", ("PUT", "/o_1/_mapping", {"properties": {"f": {"type": "nosuch"}}}), TWO, indices=CLEAN),
    Probe("t2_7_type_change", "T2-7 mapper type change", ("PUT", "/o_3/_mapping", {"properties": {"f": {"type": "long"}}}), [mk("o_3", {"mappings": {"properties": {"f": {"type": "keyword"}}}})], indices=CLEAN),
    Probe("t2_7_param_change", "T2-7 mapper parameter change", ("PUT", "/o_3/_mapping", {"properties": {"f": {"type": "text", "analyzer": "english"}}}), [mk("o_3", {"mappings": {"properties": {"f": {"type": "text", "analyzer": "standard"}}}})], indices=CLEAN),
    # --- T3 ---
    Probe("t3_2_comma_delete_missing", "T3-2 DELETE /a,missing", ("DELETE", "/o_1,o_missing"), TWO, indices=CLEAN),
    Probe("t3_2_comma_delete_missing_then", "T3-2 DELETE /a,missing: does a survive?", ("GET", "/o_1/_count"), TWO + [("DELETE", "/o_1,o_missing")], view=status_view, indices=CLEAN),
    Probe("t3_4_create_named_like_alias", "T3-4 create an index named like an alias", mk("o_a"), NO_WRITE_ALIAS, indices=CLEAN),
    Probe("t3_6_alias_name_underscore", "T3-6 alias name rules (leading _)", ("PUT", "/o_1/_alias/_bad"), TWO, indices=CLEAN),
    Probe("t3_6_alias_name_upper", "T3-6 alias name rules (uppercase)", ("PUT", "/o_1/_alias/Upper"), TWO, view=status_view, indices=CLEAN),
    Probe("t3_6_alias_name_hash", "T3-6 alias name rules (#)", ("PUT", "/o_1/_alias/a%23b"), TWO, indices=CLEAN),
    Probe("t3_6_alias_equals_index", "T3-6 alias named like an index", ("PUT", "/o_1/_alias/o_2"), TWO, indices=CLEAN),
    Probe("t3_6_remove_missing_pair", "T3-6 remove of a missing pair", ("POST", "/_aliases", {"actions": [{"remove": {"index": "o_1", "alias": "o_zz"}}]}), TWO, indices=CLEAN),
    Probe("t3_6_delete_alias_no_member", "T3-6 DELETE /a,b/_alias/x when neither is a member", ("DELETE", "/o_1,o_2/_alias/o_zz"), TWO, indices=CLEAN),
    Probe("t3_6_get_alias_missing", "T3-6 GET /_alias/{missing}", ("GET", "/_alias/o_zz"), TWO, view=body_view, indices=CLEAN),
    Probe("t3_6_get_alias_partly_missing", "T3-6 partly missing GET /_alias/a,b", ("GET", "/_alias/o_a,o_zz"), NO_WRITE_ALIAS, view=body_view, indices=CLEAN),
    Probe("t3_6_get_index_alias_lists_bare", "T3-6 GET /{index}/_alias of an index without aliases", ("GET", "/o_1/_alias"), TWO, view=body_view, indices=CLEAN),
    Probe("t3_6_alias_in_action_index", "T3-6 an alias name in an action's index", ("POST", "/_aliases", {"actions": [{"add": {"index": "o_a", "alias": "o_b"}}]}), NO_WRITE_ALIAS, indices=CLEAN),
    Probe("t3_6_action_missing_index", "T3-6 add naming a missing index", ("POST", "/_aliases", {"actions": [{"add": {"index": "o_missing", "alias": "o_b"}}]}), TWO, indices=CLEAN),
    Probe("t3_7_two_write_indices", "T3-7 two write indices", ("POST", "/_aliases", {"actions": [{"add": {"index": "o_1", "alias": "o_w", "is_write_index": True}}, {"add": {"index": "o_2", "alias": "o_w", "is_write_index": True}}]}), TWO, indices=CLEAN, view=unordered_view),
    Probe("t3_8_create_unknown_key", "T3-8 create-body unknown key", mk("o_3", {"zzz": {}}), indices=CLEAN),
    Probe("t3_8_non_json_body", "T3-8 a non-JSON body", ("PUT", "/o_3", None, {"raw": b"{not json"}), indices=CLEAN, deviation="T11-5 (T4-5): JSON parse errors carry serde_json's text, not Jackson's"),
    Probe("t3_8_put_mapping_no_body", "T3-8 PUT _mapping without a body", ("PUT", "/o_1/_mapping"), TWO, indices=CLEAN),
    Probe("t3_8_unknown_features", "T3-8 unknown features value", ("GET", "/o_1?features=xyz"), TWO, indices=CLEAN),
    # --- T4 ---
    Probe("t4_5_metadata_field", "T4-5 metadata field in a document", ("PUT", "/o_1/_doc/1", {"_id": "x"}), TWO, indices=CLEAN),
    Probe("t4_5_bad_long", "T4-5 a value that does not fit its field", ("PUT", "/o_3/_doc/1", {"n": "abc"}), [mk("o_3", {"mappings": {"properties": {"n": {"type": "long"}}}})], indices=CLEAN),
    Probe("t4_5_update_unknown_key", "T4-5 unknown _update body key", ("POST", "/o_1/_update/1", {"zzz": 1}), TWO, indices=CLEAN),
    Probe("t4_5_refresh_bogus", "T4-5 refresh value", ("PUT", "/o_1/_doc/1?refresh=bogus", {"a": 1}), TWO, indices=CLEAN),
    Probe("t4_5_op_type_bogus", "T4-5 op_type value", ("PUT", "/o_1/_doc/1?op_type=bogus", {"a": 1}), TWO, indices=CLEAN),
    Probe("t4_5_bodyless_index", "T4-5 body-less index request", ("PUT", "/o_1/_doc/1"), TWO, indices=CLEAN),
    Probe("t4_5_bodyless_update", "T4-5 body-less update request", ("POST", "/o_1/_update/1"), TWO, indices=CLEAN),
    Probe("t4_5_empty_update", "T4-5 update with an empty body object", ("POST", "/o_1/_update/1", {}), TWO, indices=CLEAN),
    Probe("t4_5_id_too_long", "T4-5 id-length error", ("PUT", "/o_1/_doc/" + "x" * 600, {"a": 1}), TWO, indices=CLEAN),
    Probe("t4_5_binary_not_base64", "Task 4 binary value that is not base64", ("PUT", "/o_3/_doc/1", {"b": "***"}), [mk("o_3", {"mappings": {"properties": {"b": {"type": "binary"}}}})], view=status_view, indices=CLEAN),
    Probe("t4_5_dot_product_write", "Task 4 a non-unit vector written to a dot_product field", ("PUT", "/o_3/_doc/1", {"v": [1.0, 1.0]}), [mk("o_3", {"mappings": {"properties": {"v": {"type": "dense_vector", "dims": 2, "similarity": "dot_product"}}}})], indices=CLEAN),
    Probe("t4_5_cosine_zero_write", "Task 4 a zero vector written to a cosine field", ("PUT", "/o_3/_doc/1", {"v": [0, 0, 0]}), [mk("o_3", {"mappings": {"properties": {"v": {"type": "dense_vector", "dims": 3, "similarity": "cosine"}}}})], indices=CLEAN),
    Probe("t4_5_dims_mismatch_write", "Task 4 a vector of the wrong length", ("PUT", "/o_3/_doc/1", {"v": [1, 0]}), [mk("o_3", {"mappings": {"properties": {"v": {"type": "dense_vector", "dims": 3, "similarity": "cosine"}}}})], indices=CLEAN),
    Probe("t4_5_vector_not_array_write", "Task 4 a vector that is not an array", ("PUT", "/o_3/_doc/1", {"v": "abc"}), [mk("o_3", {"mappings": {"properties": {"v": {"type": "dense_vector", "dims": 3, "similarity": "cosine"}}}})], indices=CLEAN, deviation="T11-5: a non-array vector is a field parse error; ES reports its token parser's text"),
    Probe("t4_3_null_vector", "T4-3 a null vector stays in _source", ("GET", "/o_3/_doc/1"), [mk("o_3", {"mappings": {"properties": {"v": {"type": "dense_vector", "dims": 3}}}}), doc("o_3", "1", {"v": None, "t": "x"})], view=source_view, indices=CLEAN),
    Probe("t4_11_update_missing_index", "T4-11 update on a missing index", ("POST", "/o_missing/_update/1", {"doc": {"a": 1}}), view=lambda a: {k: v for k, v in error_view(a).items()}),
    Probe("t4_11_update_missing_index_creates", "T4-11 update on a missing index creates it", ("HEAD", "/o_missing"), [("POST", "/o_missing/_update/1", {"doc": {"a": 1}})], view=status_view),
    Probe("t4_11_delete_missing_index", "T4-11 delete on a missing index", ("DELETE", "/o_missing/_doc/1")),
    # --- T5-3 ---
    Probe("t5_3_empty_bulk", "T5-3 empty bulk", ("POST", "/_bulk", None, {"raw": b"\n\n", "content_type": NDJSON})),
    Probe("t5_3_non_json_action", "T5-3 action line that is not JSON", ("POST", "/_bulk", None, {"raw": b"nope\n{}\n", "content_type": NDJSON}), deviation="T11-5 (T4-5): JSON parse errors carry serde_json's text, not Jackson's"),
    Probe("t5_3_action_second_key", "T5-3 action object with a second key", ("POST", "/_bulk", None, {"raw": b'{"index":{"_index":"o_1","_id":"k"},"delete":{}}\n{"a":1}\n', "content_type": NDJSON}), TWO, view=bulk_view, indices=CLEAN),
    Probe("t5_3_no_trailing_newline", "T5-3 bulk without the trailing newline", ("POST", "/_bulk", None, {"raw": b'{"index":{"_index":"o_1","_id":"k"}}\n{"a":1}', "content_type": NDJSON}), TWO, indices=CLEAN),
    Probe("t5_3_update_without_id", "T5-3 update without _id", ("POST", "/_bulk", [{"update": {"_index": "o_1"}}, {"doc": {"a": 1}}], {"content_type": NDJSON}), TWO, view=bulk_view, indices=CLEAN),
    Probe("t5_3_delete_without_id", "T5-3 delete without _id", ("POST", "/_bulk", [{"delete": {"_index": "o_1"}}], {"content_type": NDJSON}), TWO, view=bulk_view, indices=CLEAN),
    # --- T6 ---
    Probe(
        "t6_3_excludes_empty_object_and_array",
        "T6-3 empty object kept / empty array dropped under excludes",
        ("GET", "/o_3/_doc/1?_source_excludes=a.b,arr.c,objarr.c"),
        [mk("o_3"), doc("o_3", "1", {"a": {"b": 1}, "arr": [{"c": 1}], "objarr": [{"c": 1}, {"c": 2, "d": 3}], "keep": 1})],
        view=source_view,
        indices=CLEAN,
    ),
    Probe("t6_3_excludes_keep_original_empty", "T6-3 an originally empty object and array stay", ("GET", "/o_3/_doc/e?_source_excludes=a.b"), [mk("o_3"), doc("o_3", "e", {"a": {"b": 1}, "e": {}, "arr": [], "x": 1, "m": {"n": {"o": 1}}})], view=source_view, indices=CLEAN),
    Probe("t6_3_include_then_exclude_all", "T6-3 includes m, excludes m.n.o", ("GET", "/o_3/_doc/e?_source_includes=m&_source_excludes=m.n.o"), [mk("o_3"), doc("o_3", "e", {"a": {"b": 1}, "e": {}, "arr": [], "x": 1, "m": {"n": {"o": 1}}})], view=source_view, indices=CLEAN),
    Probe("t6_3_includes_empty", "T6-3 includes of empty values", ("GET", "/o_3/_doc/e?_source_includes=e,arr,x"), [mk("o_3"), doc("o_3", "e", {"a": {"b": 1}, "e": {}, "arr": [], "x": 1, "m": {"n": {"o": 1}}})], view=source_view, indices=CLEAN),
    Probe("t6_5_source_false", "T6-5 _source endpoint with _source=false", ("GET", "/o_3/_source/1?_source=false"), [mk("o_3"), doc("o_3", "1", {"a": 1})], indices=CLEAN),
    Probe("t6_6_mget_no_docs", "T6-6 mget with no documents", ("POST", "/o_1/_mget", {}), TWO, indices=CLEAN),
    Probe("t6_6_mget_empty_docs", "T6-6 mget with an empty docs list", ("POST", "/o_1/_mget", {"docs": []}), TWO, indices=CLEAN),
    Probe("t6_6_mget_unknown_key", "T6-6 mget unknown key", ("POST", "/o_1/_mget", {"zzz": []}), TWO, indices=CLEAN),
    Probe("t6_6_mget_unknown_entry_field", "T6-6 mget unknown entry field", ("POST", "/o_1/_mget", {"docs": [{"_id": "1", "zzz": 1}]}), TWO, indices=CLEAN),
    Probe("t6_6_mget_id_missing", "T6-6 mget entry without _id", ("POST", "/o_1/_mget", {"docs": [{"_index": "o_1"}]}), TWO, indices=CLEAN),
    Probe("t6_6_mget_index_missing", "T6-6 mget entry without an index", ("POST", "/_mget", {"docs": [{"_id": "1"}]}), view=mget_view),
    Probe("t6_6_mget_index_missing_second", "T6-6 mget, second entry without an index", ("POST", "/_mget", {"docs": [{"_index": "o_1", "_id": "0"}, {"_id": "1"}]}), TWO, view=mget_view, indices=CLEAN),
    Probe("t6_6_mget_ids_without_index", "T6-6 mget ids without an index", ("POST", "/_mget", {"ids": ["1"]}), view=mget_view),
    Probe("t6_6_mget_entry_index_not_found", "T6-6 mget entry whose index does not exist", ("POST", "/_mget", {"docs": [{"_index": "o_missing", "_id": "1"}, {"_index": "o_1", "_id": "1"}]}), TWO, view=mget_view, indices=CLEAN),
]

SEARCH_MAPPING = {
    "mappings": {
        "properties": {
            "t": {"type": "text"},
            "k": {"type": "keyword"},
            "n": {"type": "long"},
            "d": {"type": "date"},
            "dot": {"type": "dense_vector", "dims": 2, "index": True, "similarity": "dot_product"},
            "cos": {"type": "dense_vector", "dims": 2, "index": True, "similarity": "cosine"},
            "hidden": {"type": "text", "index": False},
            "bin": {"type": "binary"},
        }
    }
}
SEARCH = [
    mk("o_3", SEARCH_MAPPING),
    ("POST", "/o_3/_bulk?refresh=true", [
        {"index": {"_id": "1"}}, {"t": "quick fox", "k": "a", "n": 1, "d": "2026-09-24T10:00:00Z", "dot": [0.6, 0.8], "cos": [1, 0], "hidden": "x", "bin": "AAAA"},
        {"index": {"_id": "2"}}, {"t": "lazy dog", "k": "b", "n": 2, "d": "2026-09-24T23:59:59.999Z", "dot": [1, 0], "cos": [0, 1]},
        {"index": {"_id": "3"}}, {"t": "quick dog", "k": "c", "n": 3, "d": "2026-09-25T00:00:00Z", "dot": [0, 1], "cos": [1, 1]},
        {"index": {"_id": "4"}}, {"t": "slow cat", "k": "d", "n": 4, "d": "2026-10-31T12:00:00Z"},
    ]),
]
S = ("o_3", "o_4")


def search(body, path="/o_3/_search"):
    return ("POST", path, body)


def nested_bool(depth):
    q = {"match_all": {}}
    for _ in range(depth - 1):
        q = {"bool": {"must": [q]}}
    return q


def _rest(a):
    return {k: v for k, v in error_view(a).items() if k != "status"}


def shards_view(a):
    body = a.body if isinstance(a.body, dict) else {}
    return {"status": a.status, "shards": body.get("_shards"), "total": body.get("hits", {}).get("total"), **_rest(a)}


PROBES += [
    # --- T7-3 dates ---
    Probe("t7_3_term_date_whole_day", "T7-3 term on a date matches the whole day", search({"query": {"term": {"d": "2026-09-24"}}}), SEARCH, view=hits_view, indices=S),
    Probe("t7_3_range_lte_month_rounds_up", "T7-3 bare-date round-up fill (lte 2026-10)", search({"query": {"range": {"d": {"lte": "2026-10"}}}}), SEARCH, view=hits_view, indices=S),
    Probe("t7_3_range_lt_month", "T7-3 bare-date lt 2026-10", search({"query": {"range": {"d": {"lt": "2026-10"}}}}), SEARCH, view=hits_view, indices=S),
    Probe("t7_3_range_gt_day", "T7-3 bare-date gt 2026-09-24", search({"query": {"range": {"d": {"gt": "2026-09-24"}}}}), SEARCH, view=hits_view, indices=S),
    Probe("t7_3_range_lte_year", "T7-3 bare-date lte 2026", search({"query": {"range": {"d": {"lte": "2026"}}}}), SEARCH, view=hits_view, indices=S),
    Probe("t7_3_range_gt_month", "T7-3 bare-date gt 2026-09", search({"query": {"range": {"d": {"gt": "2026-09"}}}}), SEARCH, view=hits_view, indices=S),
    Probe("t7_3_term_month", "T7-3 term on a bare month", search({"query": {"term": {"d": "2026-09"}}}), SEARCH, view=hits_view, indices=S),
    Probe("t7_3_range_anchor_round", "T7-3 date math with rounding (2026-09-24||/d both bounds)", search({"query": {"range": {"d": {"gte": "2026-09-24||/d", "lte": "2026-09-24||/d"}}}}), SEARCH, view=hits_view, indices=S),
    # --- T7-6 ---
    Probe("t7_6_dot_product_unit", "T7-6 dot_product knn query vector must be unit length", search({"knn": {"field": "dot", "query_vector": [3, 4], "k": 2, "num_candidates": 10}}), SEARCH, indices=S),
    Probe("t7_6_dot_product_unit_query", "T7-6 dot_product knn query (as a query clause)", search({"query": {"knn": {"field": "dot", "query_vector": [3, 4], "k": 2}}}), SEARCH, indices=S),
    # --- T7-7 texts ---
    Probe("t7_7_malformed", "T7-7 query malformed, no start_object", search({"query": {"match_all": []}}), SEARCH, indices=S),
    Probe("t7_7_malformed_term", "T7-7 term malformed", search({"query": {"term": ["x"]}}), SEARCH, indices=S),
    Probe("t7_7_multiple_fields", "T7-7 multiple fields", search({"query": {"term": {"k": "a", "n": 1}}}), SEARCH, indices=S),
    Probe("t7_7_gt_and_gte", "T7-7 gt and gte", search({"query": {"range": {"n": {"gt": 1, "gte": 1}}}}), SEARCH, view=hits_view, indices=S),
    Probe("t7_7_gte_then_gt", "T7-7 gte then gt: the last wins", search({"query": {"range": {"n": {"gte": 1, "gt": 1, "lte": 3, "lt": 3}}}}), SEARCH, view=hits_view, indices=S),
    Probe("t7_7_operator_enum", "T7-7 operator enum", search({"query": {"match": {"t": {"query": "x", "operator": "xx"}}}}), SEARCH, indices=S),
    Probe("t7_7_negative_boost", "T7-7 negative boost", search({"query": {"term": {"k": {"value": "a", "boost": -1}}}}), SEARCH, indices=S, deviation='T11-5: ES embeds its own rendering of the query in the text'),
    Probe("t7_7_knn_without_vector", "T7-7 knn without a vector", search({"knn": {"field": "cos", "k": 2, "num_candidates": 10}}), SEARCH, indices=S),
    Probe("t7_7_datemath_unit", "T7-7 date math unit", search({"query": {"range": {"d": {"gte": "now+1x"}}}}), SEARCH, indices=S),
    Probe("t7_7_datemath_operator", "T7-7 date math operator", search({"query": {"range": {"d": {"gte": "now*1d"}}}}), SEARCH, indices=S),
    Probe("t7_7_datemath_truncated", "T7-7 truncated date math", search({"query": {"range": {"d": {"gte": "now+"}}}}), SEARCH, indices=S),
    Probe("t7_7_depth_30", "T7-7 depth 30 parses", search({"query": nested_bool(30)}), SEARCH, view=hits_view, indices=S),
    Probe("t7_7_depth_31", "T7-7 depth 31 refused", search({"query": nested_bool(31)}), SEARCH, indices=S, deviation='T11-5: ES nests one x_content_parse_exception per level with positions'),
    # --- T7-4 ---
    Probe("t7_4_unindexed_text", "T7-4 unindexed text refuses a query", search({"query": {"match": {"hidden": "x"}}}), SEARCH, indices=S),
    Probe("t7_4_unindexed_text_term", "T7-4 unindexed text refuses term", search({"query": {"term": {"hidden": "x"}}}), SEARCH, indices=S),
    Probe("t7_4_unindexed_text_exists", "T7-4 exists on an unindexed text", search({"query": {"exists": {"field": "hidden"}}}), SEARCH, view=hits_view, indices=S),
    Probe("t7_4_binary_term", "T7-4 term on a binary", search({"query": {"term": {"bin": "AAAA"}}}), SEARCH, indices=S),
    Probe("t7_4_binary_exists", "T7-4 exists on a binary", search({"query": {"exists": {"field": "bin"}}}), SEARCH, view=hits_view, indices=S),
    Probe("t7_4_long_bad_string", "T7-4 non-numeric string on a long", search({"query": {"term": {"n": "abc"}}}), SEARCH, indices=S),
    # --- T8 ---
    Probe("t8_6_id_sort", "T8-6 _id sort text", search({"sort": [{"_id": "asc"}]}), SEARCH, indices=S),
    Probe("t8_7_search_after_count", "T8-7 search_after count mismatch", search({"sort": [{"n": "asc"}], "search_after": [1, 2]}), SEARCH, indices=S),
    Probe("t8_7_search_after_parse", "T8-7 search_after value of the wrong type", search({"sort": [{"n": "asc"}], "search_after": ["abc"]}), SEARCH, indices=S),
    Probe("t8_7_search_after_from", "T8-7 search_after with from", search({"sort": [{"n": "asc"}], "search_after": [1], "from": 1}), SEARCH, indices=S),
    Probe("t8_7_search_after_score", "T8-7 / Ruling 10 search_after without a sort", search({"search_after": [1.2]}), SEARCH, view=status_view, indices=S),
    Probe("t8_8_track_total_hits_negative", "T8-8 track_total_hits negative", search({"track_total_hits": -5}), SEARCH, indices=S),
    Probe("t8_8_search_type", "T8-8 search_type text", ("POST", "/o_3/_search?search_type=bogus", {}), SEARCH, indices=S),
    # --- T9 ---
    Probe("t9_3_query_string_error", "T9-3 query_string syntax error envelope", search({"query": {"query_string": {"query": "t:(quick"}}}), SEARCH, indices=S),
    Probe(
        "t9_6_partial_failure",
        "T9-6 a multi-index search where one index fails",
        search({"sort": [{"only_here": "asc"}]}, "/o_3,o_4/_search"),
        SEARCH + [mk("o_4", {"mappings": {"properties": {"only_here": {"type": "long"}}}})],
        view=lambda a: {"status": a.status, "failed": (a.body.get("_shards") or {}).get("failed") if isinstance(a.body, dict) else None, **_rest(a)},
        indices=S,
        deviation="O-M15-10 / T9-6",
    ),
    Probe("t9_7_count_unknown_key", "T9-7 _count unknown key", ("POST", "/o_3/_count", {"size": 1}), SEARCH, indices=S),
    Probe("t9_7_count_bad_query", "T9-7 _count with a shard-level query error", ("POST", "/o_3/_count", {"query": {"term": {"n": "abc"}}}), SEARCH, indices=S),
    Probe("t9_7_msearch_unknown_header", "T9-7 _msearch unknown header key", ("POST", "/_msearch", None, {"raw": b'{"index":"o_3","zzz":1}\n{}\n', "content_type": NDJSON}), SEARCH, view=msearch_view, indices=S),
    Probe("t9_7_msearch_missing_body", "T9-7 _msearch header without a body line", ("POST", "/_msearch", None, {"raw": b'{"index":"o_3"}\n', "content_type": NDJSON}), SEARCH, view=msearch_view, indices=S),
    Probe("t9_7_msearch_no_trailing_newline", "T9-7 _msearch without the trailing newline", ("POST", "/_msearch", None, {"raw": b'{"index":"o_3"}\n{}', "content_type": NDJSON}), SEARCH, view=msearch_view, indices=S),
    Probe("t9_7_msearch_header_then_pair", "T9-7 _msearch pair then a header without a body", ("POST", "/_msearch", None, {"raw": b'{"index":"o_3"}\n{}\n{"index":"o_3"}\n', "content_type": NDJSON}), SEARCH, view=msearch_view, indices=S),
    Probe("t9_7_msearch_empty", "T9-7 empty _msearch", ("POST", "/_msearch", None, {"raw": b"\n", "content_type": NDJSON})),
    Probe("t9_8_empty_resolution", "T9-8 search that resolves to no index", ("GET", "/o_nomatch*/_search"), view=shards_view),
    Probe("t9_8_ignore_unavailable", "T9-8 only missing names under ignore_unavailable", ("GET", "/o_missing/_search?ignore_unavailable=true"), view=shards_view),
    Probe("t9_8_count_empty_resolution", "T9-8 _count that resolves to no index", ("GET", "/o_nomatch*/_count"), view=body_view),
    Probe(
        "t9_9_doc_sort_value",
        "T9-9 _doc sort value",
        search({"sort": ["_doc"], "size": 1}),
        SEARCH,
        view=lambda a: {"status": a.status, "sort": [type(v).__name__ for v in a.body["hits"]["hits"][0]["sort"]] if a.status == 200 else None},
        indices=S,
        deviation="T9-9",
    ),
    # --- T9a-6 texts (by-query requests) and Task 10 ---
    Probe("t9a_6_ubq_no_query", "T9a-6 ubq without a query", ("POST", "/o_3/_update_by_query", {"script": {"source": "ctx._source.remove('k')"}}), SEARCH, view=lambda a: {"status": a.status, **_rest(a)}, indices=S),
    Probe("t9a_6_dbq_no_query", "T9a-6 dbq without a query", ("POST", "/o_3/_delete_by_query", {}), SEARCH, indices=S),
    Probe("t9a_6_dbq_no_body", "T9a-6 dbq without a body", ("POST", "/o_3/_delete_by_query"), SEARCH, indices=S),
    Probe("t9a_6_unknown_body_key", "T9a-6 request does not support [k]", ("POST", "/o_3/_delete_by_query", {"query": {"match_all": {}}, "zzz": 1}), SEARCH, indices=S),
    Probe("t9a_6_unknown_body_key_object", "T9a-6 unknown body key holding an object", ("POST", "/o_3/_delete_by_query", {"query": {"match_all": {}}, "zzz": {}}), SEARCH, indices=S),
    Probe("t9a_6_max_docs_negative", "T9a-6 max_docs negative", ("POST", "/o_3/_delete_by_query?max_docs=-2", {"query": {"match_all": {}}}), SEARCH, indices=S),
    Probe("t9a_6_conflicts", "T9a-6 conflicts text", ("POST", "/o_3/_delete_by_query?conflicts=bogus", {"query": {"match_all": {}}}), SEARCH, indices=S),
    Probe("t9a_6_conflicts_body", "T9a-6 conflicts text (body)", ("POST", "/o_3/_delete_by_query", {"query": {"match_all": {}}, "conflicts": "bogus"}), SEARCH, indices=S),
    Probe("t9a_6_max_docs_zero", "T9a-6 maxDocs text", ("POST", "/o_3/_delete_by_query?max_docs=0", {"query": {"match_all": {}}}), SEARCH, indices=S),
    Probe("t9a_6_max_docs_body_zero", "T9a-6 maxDocs text (body)", ("POST", "/o_3/_delete_by_query", {"query": {"match_all": {}}, "max_docs": 0}), SEARCH, indices=S),
    Probe("t9a_6_scroll_size_zero", "T9a-6 scroll_size range text (0)", ("POST", "/o_3/_delete_by_query?scroll_size=0", {"query": {"match_all": {}}}), SEARCH, indices=S),
    Probe("t9a_6_scroll_size_big", "T9a-6 scroll_size range text (20000)", ("POST", "/o_3/_delete_by_query?scroll_size=20000", {"query": {"match_all": {}}}), SEARCH, indices=S),
    Probe("t9a_6_time_value", "T9a-6 time-value text", ("POST", "/o_3/_delete_by_query?timeout=5x", {"query": {"match_all": {}}}), SEARCH, indices=S),
    Probe("t10_dbq_response_keys", "Task 10 _delete_by_query response keys", ("POST", "/o_3/_delete_by_query?refresh=true", {"query": {"term": {"k": "a"}}}), SEARCH, view=lambda a: {"status": a.status, "keys": list(a.body), "deleted": a.body.get("deleted"), "total": a.body.get("total"), "batches": a.body.get("batches")}, indices=S),
    Probe("t10_ubq_response_keys", "Task 9a _update_by_query response keys", ("POST", "/o_3/_update_by_query?refresh=true", {"query": {"term": {"k": "a"}}, "script": {"source": "ctx._source.k = params.v", "params": {"v": "z"}}}), SEARCH, view=lambda a: {"status": a.status, "keys": list(a.body), "updated": a.body.get("updated"), "total": a.body.get("total")}, indices=S),
    Probe("t10_dbq_missing_index", "Task 10 dbq on a missing index", ("POST", "/o_missing/_delete_by_query", {"query": {"match_all": {}}})),
    Probe("t10_dbq_through_alias", "Task 10 dbq through a multi-member alias", ("POST", "/o_a/_delete_by_query?refresh=true", {"query": {"match_all": {}}}), NO_WRITE_ALIAS + [doc("o_1", "1", {"a": 1}), doc("o_2", "2", {"a": 2}), doc("o_2", "3", {"a": 3})], view=lambda a: {"status": a.status, "deleted": a.body.get("deleted"), "total": a.body.get("total"), "batches": a.body.get("batches")}, indices=CLEAN, deviation='T11-5 (T10-2): batches are counted per index'),
]


def main(argv):
    loams, oracle = argv[1], argv[2]
    only = set(argv[3:])
    same = differ = 0
    for probe in PROBES:
        if only and probe.id not in only:
            continue
        a = run_probe(loams, probe)
        b = run_probe(oracle, probe)
        if a == b:
            same += 1
            print(f"SAME   {probe.id}")
        else:
            differ += 1
            tag = "KEPT  " if probe.deviation else "DIFF  "
            print(f"{tag} {probe.id}  ({probe.item})")
            print(f"   loams: {json.dumps(a)}")
            print(f"   oracle: {json.dumps(b)}")
    print(f"{same} same, {differ} different")


if __name__ == "__main__":
    main(sys.argv)
