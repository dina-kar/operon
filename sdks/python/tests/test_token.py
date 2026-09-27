from __future__ import annotations

import pytest

from operon import ConsistencyToken


def test_token_round_trips_its_text_form() -> None:
    text = "v1:s7/p3@918274,s7/p4@1"
    token = ConsistencyToken.parse(text)
    assert token.items == ((7, 3, 918274), (7, 4, 1))
    assert str(token) == text


def test_empty_token_has_no_items() -> None:
    token = ConsistencyToken.parse("v1:")
    assert token.items == ()
    assert str(token) == "v1:"


def test_token_items_are_sorted() -> None:
    assert str(ConsistencyToken.parse("v1:s9/p0@1,s1/p2@3")) == "v1:s1/p2@3,s9/p0@1"


def test_token_accepts_the_largest_values() -> None:
    text = f"v1:s{2**64 - 1}/p{2**32 - 1}@{2**64 - 1}"
    assert str(ConsistencyToken.parse(text)) == text


@pytest.mark.parametrize(
    "text",
    [
        "v2:s1/p0@1",
        "s1/p0@1",
        "v1:s7p3@1",
        "v1:s-1/p0@1",
        "v1:s7/p0@",
        "v1:s07/p0@1",
        "v1:s7/p4294967296@1",
        "v1:s7/p0@18446744073709551616",
        "v1:s1/p0@1,s1/p0@2",
        "v1:s1/p0@1,",
    ],
)
def test_token_rejects_malformed_text(text: str) -> None:
    with pytest.raises(ValueError, match=r"."):
        ConsistencyToken.parse(text)


def test_merge_keeps_the_highest_offset_per_partition() -> None:
    a = ConsistencyToken.parse("v1:s1/p0@5,s1/p1@2")
    b = ConsistencyToken.parse("v1:s1/p0@3,s2/p0@9")
    assert str(a.merge(b)) == "v1:s1/p0@5,s1/p1@2,s2/p0@9"
    assert str(a.merge()) == str(a)
