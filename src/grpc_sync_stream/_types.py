"""The ``EOF`` marker and type aliases shared by all modules."""

from __future__ import annotations

import enum
from collections.abc import Sequence
from typing import Final


class EOFType(enum.Enum):
    """Type of :data:`EOF`. A single-member enum, so ``x is EOF`` narrows types."""

    EOF = "EOF"

    def __repr__(self) -> str:
        return "EOF"


#: Returned by ``read()`` once the stream has ended with status OK.
EOF: Final = EOFType.EOF

#: How a call ended: ``EOF`` for status OK, otherwise the exception it raised.
Outcome = EOFType | Exception

Metadata = tuple[tuple[str, str | bytes], ...]
MetadataArg = Sequence[tuple[str, str | bytes]] | None


def as_metadata(metadata: MetadataArg) -> Metadata | None:
    return None if metadata is None else tuple(metadata)
