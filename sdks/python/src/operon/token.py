"""Consistency tokens (overview §6.5): the text form `v1:s<stream>/p<partition>@<offset>,…`."""

from __future__ import annotations

import re
from dataclasses import dataclass

__all__ = ["ConsistencyToken"]

_U64_MAX = 2**64 - 1
_U32_MAX = 2**32 - 1
_DEC = r"(0|[1-9][0-9]*)"
_ITEM = re.compile(rf"s{_DEC}/p{_DEC}@{_DEC}")
_PREFIX = "v1:"


@dataclass(frozen=True, slots=True)
class ConsistencyToken:
    """A read-your-writes token: per (stream, partition), the next offset a read must see.

    `items` are `(stream, partition, next_offset)` triples, sorted, one per
    (stream, partition).
    """

    items: tuple[tuple[int, int, int], ...] = ()

    @classmethod
    def parse(cls, text: str) -> ConsistencyToken:
        """Parses the text form; raises `ValueError` on anything outside the grammar."""
        if not isinstance(text, str):
            raise TypeError(f"a consistency token is a str, not {type(text).__name__}")
        if not text.startswith(_PREFIX):
            raise ValueError(f"not a v1 consistency token: {text!r}")
        rest = text[len(_PREFIX) :]
        if rest == "":
            return cls(())
        seen: dict[tuple[int, int], int] = {}
        for part in rest.split(","):
            match = _ITEM.fullmatch(part)
            if match is None:
                raise ValueError(f"malformed consistency token item {part!r} in {text!r}")
            stream, partition, offset = (int(g) for g in match.groups())
            if stream > _U64_MAX or offset > _U64_MAX or partition > _U32_MAX:
                raise ValueError(f"consistency token item out of range: {part!r}")
            if (stream, partition) in seen:
                raise ValueError(f"duplicate stream/partition in consistency token: {part!r}")
            seen[(stream, partition)] = offset
        return cls(tuple(sorted((s, p, o) for (s, p), o in seen.items())))

    def __str__(self) -> str:
        return _PREFIX + ",".join(f"s{s}/p{p}@{o}" for s, p, o in self.items)

    def merge(self, *others: ConsistencyToken) -> ConsistencyToken:
        """The token at least as new as every input: the highest offset per (stream, partition)."""
        best: dict[tuple[int, int], int] = {}
        for token in (self, *others):
            for s, p, o in token.items:
                key = (s, p)
                if o > best.get(key, -1):
                    best[key] = o
        return ConsistencyToken(tuple(sorted((s, p, o) for (s, p), o in best.items())))
