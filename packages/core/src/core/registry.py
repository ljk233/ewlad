"""pipeline.registry.py"""

import importlib
import pkgutil
from pathlib import Path
from typing import Callable

import polars as pl


StagingFn = Callable[[Path | str], pl.DataFrame]


staging_funcs: dict[str, StagingFn] = {}


def register_function(file: str):
    def decorator(func):
        staging_funcs[file] = func
        return func

    return decorator


def register_functions(staging):
    """
    Dynamically import modules inside data_pipeline.staging so that their
    register_staging_pipeline decorators populate staging_pipelines.
    """
    for _, module_name, _ in pkgutil.iter_modules(staging.__path__):
        importlib.import_module(f"{staging.__name__}.{module_name}")
