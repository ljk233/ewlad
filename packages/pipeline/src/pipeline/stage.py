from collections import defaultdict
from datetime import datetime as dt
from pathlib import Path
from typing import Any, Callable

from loguru import logger

from core import file_handler as fh
from core import registry
from core.env import Env


JSONLike = dict[str, Any]


def stage_files(env: Env) -> None:
    logger.info("Initializing staging")
    # Read the manfifest
    default_record = defaultdict(lambda: defaultdict(lambda: None))
    manifest = fh.read_json(env.manifest) if env.manifest.exists() else {}
    # Rebuild the manifest
    new_manifest = {}
    for file, staging_func in registry.staging_funcs.items():
        file_path = env.raw_dir / file
        new_manifest[file] = stage(
            file_path,
            env.staged_dir / f"{file_path.stem}.parquet",
            staging_func,
            manifest.get(file, default_record),
        )
    # Write the new manifest
    fh.write_json(env.manifest, new_manifest)
    logger.info("Staging completed.")
    return


def stage(
    file: Path,
    staged_file: Path,
    staging_func: Callable,
    record: JSONLike,
) -> JSONLike:
    # Early exit
    if not file.exists():
        logger.error(f"File '{file}' does not exist.")
        return {}

    # Check the manififest against the staged file
    file_hash = fh.hash_file(file)
    staged_file_hash = fh.hash_file(staged_file) if staged_file.exists() else "NO HASH"
    hash_matches = [
        file_hash == record["file_hash"],
        staged_file_hash == record["staged_file_hash"],
    ]
    if not all(hash_matches):
        try:
            staged = staging_func(file)
            staged.write_parquet(staged_file)
            logger.success(f"Staged '{file}'.")
            return {
                "file": str(file),
                "file_hash": file_hash,
                "staged_file": str(staged_file),
                "staged_file_hash": fh.hash_file(staged_file),
                "processed_at": dt.now().isoformat(),
                "staging_module": staging_func.__module__,
            }
        except Exception as err:
            msg = f"Failed to stage data with error {type(err).__name__}: {err}"
            logger.error(msg)
            return record

    logger.warning(f"Skipped '{file}': File already staged.")
    return record
