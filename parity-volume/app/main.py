"""FastAPI application: REST endpoints plus the operator web page."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from urllib.parse import quote

from .volume import DiskUnavailableError, UnrecoverableError, Volume

logger = logging.getLogger("parity_volume")

BASE_DIR = Path(os.environ.get("PV_DATA_DIR", "/data"))
DB_PATH = Path(os.environ.get("PV_DB_PATH", "/data/meta/volume.db"))
CHUNK_SIZE = int(os.environ.get("PV_CHUNK_SIZE", str(512 * 1024)))
WEB_DIR = Path(__file__).parent / "web"

app = FastAPI(title="Parity File Volume", version="1.0.0")
volume: Volume | None = None


@app.on_event("startup")
def _startup() -> None:
    global volume
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    volume = Volume(BASE_DIR, DB_PATH, chunk_size=CHUNK_SIZE)
    volume.start()


@app.on_event("shutdown")
def _shutdown() -> None:
    if volume is not None:
        volume.shutdown()


@app.exception_handler(DiskUnavailableError)
def _disk_unavailable_handler(_: Request, exc: DiskUnavailableError):
    return JSONResponse(status_code=503,
                        content={"error": "disk_unavailable", "detail": str(exc)})


@app.exception_handler(UnrecoverableError)
def _unrecoverable_handler(_: Request, exc: UnrecoverableError):
    return JSONResponse(status_code=503,
                        content={"error": "unrecoverable", "detail": str(exc)})


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse((WEB_DIR / "index.html").read_text(encoding="utf-8"))


@app.get("/api/status")
def get_status():
    return volume.status()


@app.get("/api/files")
def list_files():
    return {"files": volume.list_files()}


@app.post("/api/files")
async def upload_file(file: UploadFile):
    try:
        meta = volume.upload_file(file.file, file.filename)
    except DiskUnavailableError:
        raise
    except UnrecoverableError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"upload failed: {exc}")
    return {"file": meta}


@app.get("/api/files/{file_id}")
def download_file(file_id: str):
    try:
        meta, fh = volume.open_file_stripes(file_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="file not found")

    def iter_chunks():
        try:
            while chunk := fh.read(1024 * 1024):
                yield chunk
        finally:
            fh.close()

    # RFC 5987 encoding keeps non-ASCII filenames intact.
    disposition = (
        f"attachment; filename*=UTF-8''{quote(meta['name'], safe='')}"
    )
    return StreamingResponse(
        iter_chunks(),
        media_type="application/octet-stream",
        headers={"Content-Length": str(meta["size"]),
                 "Content-Disposition": disposition},
    )


@app.delete("/api/files/{file_id}")
def delete_file(file_id: str):
    if not volume.delete_file(file_id):
        raise HTTPException(status_code=404, detail="file not found")
    return {"deleted": file_id}


@app.post("/api/disks/{disk_index}/offline")
def offline_disk(disk_index: int):
    try:
        volume.take_disk_offline(disk_index)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except DiskUnavailableError:
        raise
    return volume.status()


@app.post("/api/disks/{disk_index}/replace")
def replace_disk(disk_index: int):
    try:
        volume.replace_disk(disk_index)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except DiskUnavailableError:
        raise
    return volume.status()


app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")
