"""pipeline/ingestor.py"""

from pathlib import Path
from typing import Optional

from loguru import logger

from core import database as db
from core import file_handler as fh
from core import manifest
from core.typing import PathStr


def run(
    manifest_file: PathStr, database: PathStr, validated_schema: PathStr
) -> Optional[Path]:
    try:
        manifest_json = fh.read_json(manifest_file)
    except Exception as err:
        msg = f"Failed to stage data with error {type(err).__name__}: {err}"
        logger.error(msg)
        return
    logger.info("Initializing ingestion")

    # Create the database
    try:
        vers_database = db.create_versioned_database(database)
        db.execute_sql_script(validated_schema, vers_database)
        logger.info(f"Created database at '{vers_database}'")
    except Exception as err:
        msg = f"Failed to create the database with error {type(err).__name__}: {err}"
        logger.error(msg)
        return

    # Iterate over the manifest
    results = []
    for record_json in manifest_json["records"]:
        record = manifest.Record.from_json(record_json)
        results.append(ingest(record, vers_database))

    n, x = len(results), sum(results)
    logger.info(f"Ingestion completed: {x}/{n} successfully ingested")

    return vers_database


def ingest(record: manifest.Record, database: PathStr):
    if not record.successful:
        msg = f"Skipped file ingestion: File was not successfully staged <{record.record_id}>"
        logger.warning(msg)
        return False

    if not record.staged_file:
        msg = f"Failed to ingest file: Staged file not found <{record.record_id}>"
        logger.warning(msg)
        return False

    table = Path(record.file).stem
    if not table in db.collect_tables(database):
        msg = f"Failed to ingest file: No staging table found <{record.record_id}>"
        logger.error(msg)
        return False

    try:
        db.load_parquet(record.staged_file, table, database)
        logger.success(f"Ingested '{record.staged_file}'.")
        return True
    except Exception as err:
        msg = f"Failed to ingest file: Error {type(err).__name__}: {err} raised in loading."
        logger.error(msg)
        return False
