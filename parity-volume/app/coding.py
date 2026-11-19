"""Stripe layout and XOR (parity) coding helpers.

A stripe is made of three blocks of identical length spread over three
independent disk directories:

* two data blocks ``d0`` / ``d1``
* one parity block  ``p = d0 XOR d1``

Any two blocks are sufficient to rebuild the third.  For the last stripe of
a file the blocks are padded with zero bytes; the stored file length in the
metadata tells how many real bytes the stripe carries.
"""

from __future__ import annotations

DISK_COUNT = 3
ROLES = ("d0", "d1", "p")


def xor_bytes(a: bytes, b: bytes) -> bytes:
    """Return the byte-wise XOR of two equal-length byte strings."""
    if len(a) != len(b):
        raise ValueError("xor_bytes requires equal length inputs")
    if not a:
        return b""
    # Big-integer XOR is implemented in C and is much faster than a Python
    # bytearray loop for block-sized inputs (hundreds of KiB).
    return (int.from_bytes(a, "big") ^ int.from_bytes(b, "big")).to_bytes(len(a), "big")


def parity(d0: bytes, d1: bytes) -> bytes:
    """Compute the parity block for two data blocks."""
    return xor_bytes(d0, d1)


def rebuild_missing(missing: str, blocks: dict[str, bytes]) -> bytes:
    """Rebuild one block from the other two.

    ``blocks`` contains the two surviving roles.  Because XOR is its own
    inverse and XORing three equal-length blocks yields zero, any missing
    block is simply the XOR of the other two.
    """
    if missing not in ROLES:
        raise ValueError(f"unknown role {missing!r}")
    present = [role for role in ROLES if role != missing]
    if any(role not in blocks for role in present):
        raise ValueError("both surviving blocks are required")
    a, b = blocks[present[0]], blocks[present[1]]
    if len(a) != len(b):
        raise ValueError("surviving blocks have differing lengths")
    return xor_bytes(a, b)
