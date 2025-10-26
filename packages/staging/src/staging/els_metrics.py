from pathlib import Path

import polars as pl

from core.registry import register_function

file = "els_metrics.csv"


@register_function(file)
def stage(file_path: Path) -> pl.DataFrame:
    return pl.read_csv(file_path)
