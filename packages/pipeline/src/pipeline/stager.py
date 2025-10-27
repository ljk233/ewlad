"""pipeline/stage.py"""

from pathlib import Path

from loguru import logger

from core import manifest
from core import registry
from core.typing import StageFn


def run(raw_dir: Path, staged_dir: Path, manifest_dir: Path) -> Path:
    if not registry.staging_funcs:
        logger.warning("Early exit: No staging functions registered")
        return Path("NO STAGING FUNCTIONS REGISTERED")

    logger.info("Initializing staging.")
    records = []
    for file, staging_func in registry.staging_funcs.items():
        file = raw_dir / file
        staged_file = staged_dir / f"{file.stem}.parquet"
        record = stage(file, staged_file, staging_func)
        records.append(record)
    n, x = len(records), sum(record.successful for record in records)
    logger.info(f"Staging completed: {x}/{n} successfully staged")

    try:
        manifiest_path = manifest.export_manifest(records, manifest_dir)
        logger.info(f"Manifest exported to '{manifiest_path}'")
        return manifiest_path
    except Exception as err:
        msg = f"Failed to export manfiest with error {type(err).__name__}: {err}"
        logger.error(msg)
        return Path("NO MANIFEST EXPORTED")


def stage(file: Path, staged_file: Path, staging_func: StageFn) -> manifest.Record:
    staging_fn_name = staging_func.__module__
    try:
        staged = staging_func(file)
        staged.write_parquet(staged_file)
        logger.success(f"Staged '{file}'.")
        return manifest.create_record(file, staging_fn_name, staged_file=staged_file)
    except Exception as err:
        msg = f"Failed to stage data with error {type(err).__name__}: {err}"
        logger.error(msg)
        return manifest.create_record(file, staging_fn_name, error=str(err))
