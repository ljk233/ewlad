"""file_handler.py"""

from datetime import datetime as dt
import hashlib
import json
import tomllib
from pathlib import Path
from typing import Any


def hash_file(path: Path | str, block_size: int = 8192) -> str:
    """Return the `sha256` hex digest of the file at `path`."""
    h = hashlib.new("sha256")
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)

    return h.hexdigest()


def read_json(file: Path | str) -> dict[str, Any]:
    path = Path(file)
    if not path.suffix.lower() == ".json":
        raise ValueError(f"File '{file}' is not a JSON file.")

    with path.open("r") as f:
        return json.load(f)


def write_json(file: Path | str, obj: dict[str, Any]) -> None:
    path = Path(file)
    if not path.suffix.lower() == ".json":
        raise ValueError(f"File '{file}' is not a JSON file.")

    with path.open("w") as f:
        json.dump(obj, f, indent=2)


def load_toml(file: Path | str) -> dict[str, Any]:
    path = Path(file)
    if not path.suffix.lower() == ".toml":
        raise ValueError(f"File '{file}' is not a TOML file.")

    with path.open("rb") as f:
        return tomllib.load(f)


def version_file(file: Path | str) -> Path:
    now = dt.now().strftime("%Y%m%d%H%M%S")
    return Path(f"{file}_{now}")
