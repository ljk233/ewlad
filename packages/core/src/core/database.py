"""database.py"""

from pathlib import Path

import duckdb
import polars as pl

from core import file_handler as fh
from core.typing import PathStr


def create_versioned_database(database: PathStr) -> Path:
    vers_database = fh.version_file_path(database)
    return create_database(vers_database)


def create_database(database: PathStr) -> Path:
    with duckdb.connect(database):
        return Path(database)


# ======================================================================
# Inspection
# ======================================================================


def collect_tables(database: PathStr) -> set[str]:
    with duckdb.connect(database) as con:
        tables = con.query("SHOW TABLES;").fetchall()
        return set(item[0] for item in tables)


def count_rows(table: str, database: PathStr) -> int:
    with duckdb.connect(database) as con:
        return con.query(f"from {table} select count(*);").fetchall()[0][0]


# ======================================================================
# I/O
# ======================================================================


def load_parquet(file_path: PathStr, table: str, database: PathStr) -> int:
    # Get the correct logic
    tables = collect_tables(database)
    if table in collect_tables(database):
        sql = f"insert into {table} from '{file_path}';"
    else:
        sql = f"create table {table} as from '{file_path}';"

    # Load the data
    prev_rows = count_rows(table, database) if table in tables else 0
    _ = execute_sql(sql, database)
    return count_rows(table, database) - prev_rows


# def load_csv_file(file_path: str, database: str, prefix: str = None) -> str:
#     table = f"{prefix if prefix else ""}{get_file_name(file_path)}"
#     sql = f"create table {table} as from '{file_path}';"
#     _ = execute_sql(sql, database)
#     return table


# ======================================================================
# Executors
# ======================================================================


def execute_sql_script(file_path: PathStr, database: PathStr) -> None:
    with Path(file_path).open("r") as f:
        execute_sql(f.read(), database)


def execute_sql(sql: str, database: PathStr) -> Path:
    with duckdb.connect(database) as con:
        con.execute(sql)

    return Path(database)


def query_database(sql: str, database: PathStr) -> pl.DataFrame:
    with duckdb.connect(database, read_only=True) as con:
        return con.sql(sql).pl()
