"""file_handler.py"""

from datetime import datetime as dt
import hashlib
import json
import tomllib
from pathlib import Path
from typing import Any


JSONLike = dict[str, Any]


def hash_file(path: Path | str, block_size: int = 8192) -> str:
    """Return the `sha256` hex digest of the file at `path`."""
    h = hashlib.new("sha256")
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)

    return h.hexdigest()


def read_json(file_path: Path | str) -> JSONLike:
    path = Path(file_path)
    if not path.parts[-1].endswith("json"):
        raise ValueError(f"File '{file_path}' is not a JSON file.")

    with open(file_path, "r") as f:
        return json.load(f)


def write_json(file: Path | str, obj: JSONLike) -> None:
    with Path(file).open("w") as fp:
        json.dump(obj, fp, indent=2)


def load_toml(file_path: Path | str) -> JSONLike:
    path = Path(file_path)
    if not path.parts[-1].endswith("toml"):
        raise ValueError(f"File '{file_path}' is not a TOML file.")

    with open(file_path, "rb") as f:
        return tomllib.load(f)


def read_file(file_path: str) -> str:
    with open(file_path, "r") as f:
        return f.read()


def version_file_path(file_path: Path | str) -> Path:
    file_path_str = str(Path(file_path))
    now = dt.now().strftime("%Y%m%d%H%M%S")
    return Path(f"{file_path_str}_{now}")
