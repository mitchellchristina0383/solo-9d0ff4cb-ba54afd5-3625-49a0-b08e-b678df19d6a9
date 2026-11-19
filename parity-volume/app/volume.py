"""Parity file volume core logic.

Implements the state machine on top of :mod:`db` and :mod:`storage`:

* streaming, crash-safe striped upload (2 data blocks + 1 XOR parity block)
* download with on-the-fly reconstruction from any two surviving blocks
* disk offlining, empty-disk replacement and background rebuild
* startup recovery: unconfirmed uploads are discarded, interrupted rebuilds
  resume automatically
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any, BinaryIO

from . import coding, storage
from .db import Database

logger = logging.getLogger("parity_volume")

# Test-only delay (seconds) applied after each rebuilt stripe so crash /
# restart scenarios can reliably interrupt an in-progress rebuild.
_REBUILD_STRIPE_DELAY = float(os.environ.get("PV_TEST_REBUILD_DELAY", "0"))


class DiskUnavailableError(Exception):
    """Raised when an operation needs a disk that is offline/missing."""


class UnrecoverableError(Exception):
    """Raised when more than one disk is missing for needed data."""


class Volume:
    def __init__(self, base_dir: str | Path, db_path: str | Path,
                 chunk_size: int = 512 * 1024):
        self.base_dir = Path(base_dir)
        self.chunk_size = chunk_size
        self.db = Database(db_path)
        self.roots = [storage.disk_root(self.base_dir, i) for i in range(coding.DISK_COUNT)]
        self._rebuild_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        # A freshly created database gets the default layout.  For an existing
        # deployment, detect vanished disk directories first so a real disk
        # loss is not masked by silently re-creating the directory.
        if not self.db.is_fresh:
            self._mark_missing_disks_offline()
        for i, root in enumerate(self.roots):
            if self.db.disk_state(i) == "online":
                storage.ensure_disk_layout(root)
        self._purge_unconfirmed_uploads()
        job = self.db.running_rebuild_job()
        if job is not None:
            self._spawn_rebuild(job["disk_index"], resume=True)

    def _reconcile_disk_presence(self) -> None:
        """Runtime check for disk directories appearing or vanishing.

        * An online disk whose directory was deleted becomes ``offline``.
        * A non-online disk whose directory exists (operator mounted a
          replacement) gets its ``blocks`` layout ensured.
        """
        for i, root in enumerate(self.roots):
            state = self.db.disk_state(i)
            if not root.is_dir():
                if state == "online":
                    logger.warning("disk%d directory vanished, marking offline", i)
                    self.db.set_disk_state(i, "offline")
            elif state != "online":
                storage.ensure_disk_layout(root)

    def shutdown(self) -> None:
        self._stop_event.set()
        thread = self._rebuild_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=30)
        self.db.close()

    def _mark_missing_disks_offline(self) -> None:
        """A disk directory that vanished (e.g. container restart without the
        volume mounted) is treated as an offline disk."""
        for i, root in enumerate(self.roots):
            if not root.is_dir():
                if self.db.disk_state(i) != "offline":
                    logger.warning("disk%d directory missing, marking offline", i)
                    self.db.set_disk_state(i, "offline")

    # ------------------------------------------------------------------ #
    # Disk operations
    # ------------------------------------------------------------------ #
    def status(self) -> dict[str, Any]:
        self._reconcile_disk_presence()
        disks = self.db.disk_states()
        job = self.db.running_rebuild_job()
        for d in disks:
            d["path"] = str(self.roots[d["disk_index"]])
            d["present"] = self.roots[d["disk_index"]].is_dir()
        return {
            "disks": disks,
            "rebuild": job,
            "chunk_size": self.chunk_size,
            "files": len(self.db.list_files()),
        }

    def take_disk_offline(self, disk_index: int) -> None:
        self._check_disk_index(disk_index)
        state = self.db.disk_state(disk_index)
        if state == "offline":
            return
        if state == "rebuilding":
            raise DiskUnavailableError(
                f"disk{disk_index} is being rebuilt and cannot be taken offline"
            )
        if self.db.running_rebuild_job() is not None:
            raise DiskUnavailableError("a rebuild is already in progress")
        self.db.set_disk_state(disk_index, "offline")
        logger.info("disk%d taken offline", disk_index)

    def replace_disk(self, disk_index: int) -> None:
        """Replace an offline disk with an empty one and start rebuilding."""
        self._check_disk_index(disk_index)
        state = self.db.disk_state(disk_index)
        if state != "offline":
            raise DiskUnavailableError(
                f"disk{disk_index} must be offline before replacement (state={state})"
            )
        if self.db.running_rebuild_job() is not None:
            raise DiskUnavailableError("a rebuild is already in progress")
        # Archive the old directory and provision an empty replacement.
        storage.replace_with_empty_disk(self.roots[disk_index])
        total = self.db.count_healthy_stripes_on_disk(disk_index)
        job_id = self.db.create_rebuild_job(disk_index, total)
        self.db.set_disk_state(disk_index, "rebuilding")
        logger.info("disk%d replaced, rebuild job %d over %d stripes",
                    disk_index, job_id, total)
        self._spawn_rebuild(disk_index)

    def _spawn_rebuild(self, disk_index: int, resume: bool = False) -> None:
        if self.db.disk_state(disk_index) != "rebuilding":
            self.db.set_disk_state(disk_index, "rebuilding")
        self._stop_event = threading.Event()
        self._rebuild_thread = threading.Thread(
            target=self._rebuild_worker,
            args=(disk_index,),
            name=f"rebuild-disk{disk_index}",
            daemon=True,
        )
        self._rebuild_thread.start()
        if resume:
            logger.info("resumed rebuild of disk%d", disk_index)

    # ------------------------------------------------------------------ #
    # Upload
    # ------------------------------------------------------------------ #
    def upload_file(self, fileobj: BinaryIO, filename: str) -> dict[str, Any]:
        file_id = uuid.uuid4().hex
        size = 0
        digest = hashlib.sha256()
        stripe_index = 0
        created = False
        try:
            self.db.create_file({
                "id": file_id,
                "name": filename,
                "size": 0,
                "chunk_size": self.chunk_size,
                "stripe_count": 0,
            })
            created = True
            while True:
                chunk = self._read_exact(fileobj, self.chunk_size, digest)
                if not chunk:
                    break
                size += len(chunk)
                self._write_stripe(file_id, stripe_index, chunk)
                stripe_index += 1
            stripe_count = stripe_index
            # Fix file size / stripe count, then make the file visible.
            with self.db.lock, self.db.conn:
                self.db.conn.execute(
                    "UPDATE files SET size=?, stripe_count=?, sha256=? WHERE id=?",
                    (size, stripe_count, digest.hexdigest(), file_id),
                )
            self.db.confirm_file(file_id)
            logger.info("uploaded %s (%d bytes, %d stripes)",
                        filename, size, stripe_count)
            return self.db.get_file(file_id)  # type: ignore[return-value]
        except Exception:
            if created:
                self._discard_file(file_id)
            raise

    @staticmethod
    def _read_exact(fileobj: BinaryIO, size: int,
                    digest: hashlib._Hash) -> bytes:
        """Read exactly ``size`` bytes unless EOF is reached."""
        parts: list[bytes] = []
        remaining = size
        while remaining:
            part = fileobj.read(remaining)
            if not part:
                break
            digest.update(part)
            parts.append(part)
            remaining -= len(part)
        return b"".join(parts)

    def _stripe_layout(self, stripe_index: int) -> tuple[int, int, int]:
        """Rotate which disk holds the parity block across stripes."""
        p_disk = stripe_index % coding.DISK_COUNT
        data_disks = [d for d in range(coding.DISK_COUNT) if d != p_disk]
        d0_disk, d1_disk = data_disks
        return d0_disk, d1_disk, p_disk

    def _write_stripe(self, file_id: str, stripe_index: int, data: bytes) -> None:
        length = len(data)
        # Zero-pad so all three blocks have identical length.
        d0_data = data + b"\x00" * (self.chunk_size - length)
        d1_data = b"\x00" * self.chunk_size
        d0, d1, p = self._stripe_layout(stripe_index)
        layout = {"d0": (d0, d0_data), "d1": (d1, d1_data),
                  "p": (p, coding.parity(d0_data, d1_data))}
        # A stripe is only marked healthy once all three blocks are durable,
        # so a crash can never leave a partial stripe flagged as healthy.
        self._reconcile_disk_presence()
        self.db.insert_stripe({
            "file_id": file_id,
            "stripe_index": stripe_index,
            "length": length,
            "d0_disk": d0,
            "d1_disk": d1,
            "p_disk": p,
        }, healthy=False)
        for role, (disk_index, payload) in layout.items():
            # An 'offline' disk has no usable directory; a 'rebuilding' disk
            # holds the empty replacement and accepts new stripes normally.
            if self.db.disk_state(disk_index) == "offline":
                raise DiskUnavailableError(
                    f"disk{disk_index} is offline; refusing partial stripe"
                )
            relpath = storage.block_relpath(file_id, stripe_index, role)
            storage.write_block(self.roots[disk_index], relpath, payload)
            self.db.insert_block({
                "file_id": file_id,
                "stripe_index": stripe_index,
                "role": role,
                "disk_index": disk_index,
                "relpath": relpath,
            })
        self.db.set_stripe_healthy(file_id, stripe_index, True)

    def _discard_file(self, file_id: str) -> None:
        """Remove every block and metadata row of an unconfirmed file."""
        for stripe in self.db.get_stripes(file_id):
            for block in self.db.get_blocks(file_id, stripe["stripe_index"]):
                storage.remove_block(self.roots[block["disk_index"]],
                                     block["relpath"])
        self.db.delete_file_rows(file_id)

    def _purge_unconfirmed_uploads(self) -> None:
        """Crash recovery: nothing unconfirmed may ever become visible."""
        for meta in self.db.uploading_files():
            logger.info("purging unconfirmed upload %s", meta["id"])
            self._discard_file(meta["id"])

    # ------------------------------------------------------------------ #
    # Download
    # ------------------------------------------------------------------ #
    def list_files(self) -> list[dict[str, Any]]:
        return self.db.list_files()

    def delete_file(self, file_id: str) -> bool:
        """Delete a file and all of its blocks. Returns False if unknown."""
        if self.db.get_file(file_id) is None:
            return False
        self._discard_file(file_id)
        return True

    def open_file_stripes(self, file_id: str) -> tuple[dict[str, Any], BinaryIO]:
        meta = self.db.get_file(file_id)
        if meta is None or meta["status"] != "confirmed":
            raise FileNotFoundError(file_id)
        stripes = self.db.get_stripes(file_id)
        if len(stripes) != meta["stripe_count"]:
            raise UnrecoverableError(
                f"metadata incomplete for {file_id}: "
                f"{len(stripes)}/{meta['stripe_count']} stripes"
            )
        from tempfile import SpooledTemporaryFile

        out = SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b")
        written = 0
        for stripe in stripes:
            data = self._read_stripe_data(file_id, stripe)
            out.write(data[:stripe["length"]])
            written += stripe["length"]
        if written != meta["size"]:
            out.close()
            raise UnrecoverableError(
                f"reconstructed size {written} != metadata size {meta['size']}"
            )
        out.seek(0)
        return meta, out

    def _read_stripe_data(self, file_id: str, stripe: dict[str, Any]) -> bytes:
        disk_of_role = {
            "d0": stripe["d0_disk"],
            "d1": stripe["d1_disk"],
            "p": stripe["p_disk"],
        }
        available: dict[str, bytes] = {}
        missing_roles: list[str] = []
        for role, disk_index in disk_of_role.items():
            block = self.db.get_block(file_id, stripe["stripe_index"], role)
            if block is None:
                missing_roles.append(role)
                continue
            state = self.db.disk_state(disk_index)
            if state == "offline" or not storage.block_exists(
                self.roots[disk_index], block["relpath"]
            ):
                missing_roles.append(role)
                continue
            available[role] = storage.read_block(self.roots[disk_index],
                                                 block["relpath"])
        if len(available) < 2:
            raise UnrecoverableError(
                f"stripe {file_id}/{stripe['stripe_index']} has only "
                f"{len(available)} readable block(s); more than one disk failed"
            )
        if len(available) == 3:
            check = coding.xor_bytes(
                coding.xor_bytes(available["d0"], available["d1"]),
                available["p"],
            )
            if any(check):
                raise UnrecoverableError(
                    f"stripe {file_id}/{stripe['stripe_index']} failed parity check"
                )
            return available["d0"]
        # Exactly two blocks: reconstruct the missing one.  If a data block is
        # missing we return the reconstructed data; otherwise use d0 directly.
        missing_role = missing_roles[0]
        rebuilt = coding.rebuild_missing(missing_role, available)
        if missing_role == "d0":
            return rebuilt
        if missing_role == "d1":
            return available["d0"]
        return available["d0"]

    # ------------------------------------------------------------------ #
    # Rebuild
    # ------------------------------------------------------------------ #
    def _rebuild_worker(self, disk_index: int) -> None:
        job = self.db.running_rebuild_job()
        if job is None or job["disk_index"] != disk_index:
            logger.error("no running rebuild job for disk%d", disk_index)
            return
        job_id = job["id"]
        done = 0
        bytes_done = 0
        try:
            for stripe in self.db.healthy_stripes_on_disk(disk_index):
                if self._stop_event.is_set():
                    logger.info("rebuild of disk%d interrupted", disk_index)
                    return
                self._rebuild_stripe(disk_index, stripe)
                if _REBUILD_STRIPE_DELAY:
                    time.sleep(_REBUILD_STRIPE_DELAY)
                done += 1
                bytes_done += self.chunk_size
                if done % 8 == 0:
                    self.db.rebuild_progress(job_id, done, bytes_done)
            self.db.rebuild_progress(job_id, done, bytes_done)
            # Re-scan: stripes created by uploads interleaved with the rebuild
            # must all have a block on the new disk before we declare success.
            remaining = self._stripes_missing_on_disk(disk_index)
            if remaining:
                raise UnrecoverableError(
                    f"{len(remaining)} stripes still missing on disk{disk_index}"
                )
            # Interleaved uploads may have added stripes beyond the original
            # total; sync the counters so progress ends at exactly 100%.
            final_total = self.db.count_healthy_stripes_on_disk(disk_index)
            self.db.rebuild_progress(job_id, final_total,
                                     bytes_done, total=final_total)
            self.db.finish_rebuild_job(job_id, "done", None)
            self.db.set_disk_state(disk_index, "online")
            logger.info("rebuild of disk%d complete (%d stripes)",
                        disk_index, done)
        except Exception as exc:  # noqa: BLE001 - recorded on the job
            logger.exception("rebuild of disk%d failed", disk_index)
            self.db.finish_rebuild_job(job_id, "failed", str(exc))
            self.db.set_disk_state(disk_index, "offline")

    def _rebuild_stripe(self, disk_index: int, stripe: dict[str, Any]) -> None:
        file_id = stripe["file_id"]
        idx = stripe["stripe_index"]
        disk_of_role = {
            "d0": stripe["d0_disk"],
            "d1": stripe["d1_disk"],
            "p": stripe["p_disk"],
        }
        role_on_disk = next(
            role for role, disk in disk_of_role.items() if disk == disk_index
        )
        survivors: dict[str, bytes] = {}
        for role, other_disk in disk_of_role.items():
            if role == role_on_disk:
                continue
            block = self.db.get_block(file_id, idx, role)
            if block is None or self.db.disk_state(other_disk) != "online":
                raise UnrecoverableError(
                    f"cannot rebuild stripe {file_id}/{idx}: disk{other_disk} "
                    f"for role {role} is unavailable"
                )
            if not storage.block_exists(self.roots[other_disk], block["relpath"]):
                raise UnrecoverableError(
                    f"block file missing on disk{other_disk} for "
                    f"stripe {file_id}/{idx}"
                )
            survivors[role] = storage.read_block(self.roots[other_disk],
                                                 block["relpath"])
        rebuilt = coding.rebuild_missing(role_on_disk, survivors)
        relpath = storage.block_relpath(file_id, idx, role_on_disk)
        # The replacement disk was marked rebuilding; it is writable here.
        storage.write_block(self.roots[disk_index], relpath, rebuilt)
        block = self.db.get_block(file_id, idx, role_on_disk)
        if block is None:
            self.db.insert_block({
                "file_id": file_id,
                "stripe_index": idx,
                "role": role_on_disk,
                "disk_index": disk_index,
                "relpath": relpath,
            })

    def _stripes_missing_on_disk(self, disk_index: int) -> list[dict[str, Any]]:
        missing: list[dict[str, Any]] = []
        for stripe in self.db.healthy_stripes_on_disk(disk_index):
            role_on_disk = next(
                role for role, disk in (
                    ("d0", stripe["d0_disk"]),
                    ("d1", stripe["d1_disk"]),
                    ("p", stripe["p_disk"]),
                ) if disk == disk_index
            )
            block = self.db.get_block(stripe["file_id"],
                                      stripe["stripe_index"], role_on_disk)
            if block is None or not storage.block_exists(
                self.roots[disk_index], block["relpath"]
            ):
                missing.append(stripe)
        return missing

    # ------------------------------------------------------------------ #
    def _check_disk_index(self, disk_index: int) -> None:
        if disk_index not in range(coding.DISK_COUNT):
            raise ValueError(f"disk index must be 0..{coding.DISK_COUNT - 1}")
