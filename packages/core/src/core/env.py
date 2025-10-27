"""core/env.py"""

from pathlib import Path
from typing import NamedTuple

from loguru import logger

import dotenv


class Env(NamedTuple):
    raw: Path
    staged: Path
    manifest: Path
    database: Path
    sql: Path


def create_env() -> Env:
    if not dotenv.load_dotenv():
        raise OSError("No .env file found.")

    parsed_vars = {k.lower(): Path(v) for k, v in dotenv.dotenv_values().items() if v}

    for path in parsed_vars.values():
        if path.exists() or not path.suffix:
            continue
        path.mkdir(parents=True)
        logger.info(f"Created directory '{str(path)}'")

    try:
        return Env(**parsed_vars)
    except Exception as err:
        msg = f"Failed load .env with error {type(err).__name__}: {err}"
        raise OSError(msg)
