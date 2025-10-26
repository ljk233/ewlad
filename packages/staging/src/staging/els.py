from pathlib import Path

import polars as pl

from core.registry import register_function

file = "els.xlsx"


@register_function(file)
def stage(file_path: Path) -> pl.DataFrame:
    data_dict = pl.read_excel(file_path, sheet_id=0, has_header=False)
    cleansed = [
        clean(data, sheet_name)
        for sheet_name, data in data_dict.items()
        if sheet_name not in ("Cover_sheet", "Table_of_contents", "Notes")
    ]
    return pl.concat(cleansed)


def clean(data: pl.DataFrame, sheet_name: str) -> pl.DataFrame:
    rowed = data.with_row_index()
    header_row_pred = pl.col("column_1") == "Area code"
    header_row = rowed.filter(header_row_pred).select(header_row_index="index")

    if header_row.is_empty():
        raise ValueError(f"Header row not found in sheet '{sheet_name}'")

    return (
        rowed.join(header_row, how="cross")
        .filter(pl.col("index") >= pl.col("header_row_index") + 1)
        .select(
            local_authority_district="column_1",
            year=pl.col("column_3").str.extract(r"^(\d{4})").cast(pl.Int64),
            value=pl.col("column_4").cast(pl.Float64),
            sheet_id=pl.lit(sheet_name).cast(pl.Utf8),
        )
        .drop_nulls()
    )
