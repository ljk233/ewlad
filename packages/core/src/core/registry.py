"""core/registry.py"""

import functools
import importlib
import pkgutil

from loguru import logger

from core.typing import StagingFuncMap


staging_funcs: StagingFuncMap = {}


def register_staging_func(file_key: str):
    def decorator(func):
        if file_key in staging_funcs:
            raise KeyError(f"Staging function for '{file_key}' already registered.")

        staging_funcs[file_key] = func
        return functools.wraps(func)(func)

    return decorator


def populate_registry(pkg):
    n = 0
    for _, module_name, _ in pkgutil.iter_modules(pkg.__path__):
        try:
            importlib.import_module(f"{pkg.__name__}.{module_name}")
            n += 1
        except Exception as err:
            logger.error(f"Error importing module {pkg.__name__}.{module_name}: {err}")
    logger.info(f"Imported {n} module(s) from {pkg.__name__}")
