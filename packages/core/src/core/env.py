"""core/env.py"""

import os

from pathlib import Path
from typing import NamedTuple

import dotenv


class Env(NamedTuple):
    raw_dir: Path
    staged_dir: Path
    manifest: Path


def create_env() -> Env:
    if not dotenv.load_dotenv():
        raise OSError("No .env file found.")

    parsed_vars = {k.lower(): Path(v) for k, v in dotenv.dotenv_values().items() if v}

    try:
        return Env(**parsed_vars)
    except Exception as err:
        msg = f"Failed load .env with error {type(err).__name__}: {err}"
        raise OSError(msg)
