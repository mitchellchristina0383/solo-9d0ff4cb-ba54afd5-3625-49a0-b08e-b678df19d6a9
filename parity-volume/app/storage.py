"""Physical block storage on the three persistent disk directories.

Each disk directory holds block files under ``blocks/``.  Writes are atomic
(tmp file + fsync + rename) so a crash never leaves a partially written block
that could be mistaken for a healthy one.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

BLOCKS_SUBDIR = "blocks"
_TMP_SUFFIX = ".tmp"


def block_relpath(file_id: str, stripe_index: int, role: str) -> str:
    """Return the relative path of a block inside its disk directory."""
    prefix = file_id[:2]
    return os.path.join(BLOCKS_SUBDIR, prefix, f"{file_id}_{stripe_index}_{role}.block")


def disk_root(base_dir: Path, disk_index: int) -> Path:
    return Path(base_dir) / f"disk{disk_index}"


def ensure_disk_layout(root: Path) -> Path:
    blocks_dir = Path(root) / BLOCKS_SUBDIR
    blocks_dir.mkdir(parents=True, exist_ok=True)
    return blocks_dir


def block_path(root: Path, relpath: str) -> Path:
    return Path(root) / relpath


def write_block(root: Path, relpath: str, data: bytes) -> Path:
    """Atomically write a block file and persist it to disk."""
    target = Path(root) / relpath
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + _TMP_SUFFIX)
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, target)
    return target


def read_block(root: Path, relpath: str) -> bytes:
    with open(Path(root) / relpath, "rb") as fh:
        return fh.read()


def block_exists(root: Path, relpath: str) -> bool:
    return (Path(root) / relpath).is_file()


def remove_block(root: Path, relpath: str) -> bool:
    target = Path(root) / relpath
    try:
        target.unlink()
        return True
    except FileNotFoundError:
        return False


def replace_with_empty_disk(root: Path) -> Path:
    """Archive an old disk directory and create an empty replacement.

    The old directory (if present) is renamed aside instead of being deleted,
    so an operator can inspect it.  A fresh, empty disk directory is then
    created at the original path.
    """
    root = Path(root)
    if root.exists():
        # Include a uuid so repeated replacements within the same second do
        # not collide on an already-existing archive directory.
        archived = root.with_name(
            root.name + f".failed-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        )
        os.rename(root, archived)
    ensure_disk_layout(root)
    return root
