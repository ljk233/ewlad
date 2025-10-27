"""core/manifest.py"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime as dt
from pathlib import Path
from typing import Optional, Self

from core import file_handler as fh
from core.typing import JSONLike, PathStr


@dataclass
class Record:
    file: str
    staging_module: str
    record_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    processed_at: str = field(default_factory=dt.now().isoformat)
    successful: bool = False
    file_hash: Optional[str] = None
    staged_file: Optional[str] = None
    staged_file_hash: Optional[str] = None
    error: Optional[str] = None

    @classmethod
    def from_json(cls, json: JSONLike) -> Self:
        return cls(**json)

    @property
    def base_data(self) -> JSONLike:
        return {
            "record_id": self.record_id,
            "processed_at": self.processed_at,
            "successful": self.successful,
            "file": self.file,
            "staging_module": self.staging_module,
        }

    @property
    def conditional_data(self) -> JSONLike:
        if self.successful:
            return {
                "file_hash": self.file_hash,
                "staged_file": self.staged_file,
                "staged_file_hash": self.staged_file_hash,
            }
        return {"error": self.error}

    def jsonify(self) -> JSONLike:
        return {**self.base_data, **self.conditional_data}


def export_manifest(records: list[Record], dir_path: PathStr) -> Path:
    manifest = create_manifest(records)
    file_path = Path(dir_path) / create_file_name(manifest["manifest_id"])
    fh.write_json(file_path, manifest)
    return file_path


def create_manifest(records: list[Record]) -> JSONLike:
    return {
        "manifest_id": uuid.uuid4().hex,
        "created_at": dt.now().isoformat(),
        "records": [record.jsonify() for record in records],
    }


def create_record(
    file: PathStr,
    staging_module: str,
    staged_file: PathStr = "",
    error: str = "",
) -> Record:
    if staged_file and error:
        msg = f"Record not created: Both a staged file path '{staged_file}' and error '{error}' was passed."
        raise ValueError(msg)

    if staged_file:
        return Record(
            str(file),
            staging_module,
            successful=True,
            file_hash=fh.hash_file(file),
            staged_file=str(staged_file),
            staged_file_hash=fh.hash_file(staged_file),
        )

    return Record(
        str(file),
        staging_module,
        file_hash=fh.hash_file(file) if Path(file).exists() else "No hash",
        error=error,
    )


def create_file_name(manifest_id: str) -> str:
    timestamp = dt.now().strftime("%Y%m%d%H%M%S")
    return f"{timestamp}_manifest_{manifest_id}.json"
