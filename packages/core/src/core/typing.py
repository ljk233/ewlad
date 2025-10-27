"""core/types.py"""

from pathlib import Path
from typing import Any, Callable

from polars import DataFrame

JSONLike = dict[str, Any]
PathStr = Path | str

StageFn = Callable[[PathStr], DataFrame]
StagingFuncMap = dict[str, StageFn]
