from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Callable
from typing import List
from typing import Optional
from typing import Protocol
from typing import runtime_checkable

import pandas as pd


@runtime_checkable
class Source(Protocol):
    def read(self) -> pd.DataFrame: ...


@runtime_checkable
class Sink(Protocol):
    def write(self, df: pd.DataFrame) -> None: ...


@dataclass
class PipelineResult:
    name: str
    description: str
    output_blob: str
    input_rows: int
    output_rows: int
    duration_seconds: float
    status: str  # "ok" | "failed"
    error: Optional[str] = None


@dataclass
class TaskConfig:
    name: str
    description: str
    output_blob: str
    source: Source
    transformations: List[Callable[[pd.DataFrame], pd.DataFrame]] = field(default_factory=list)
    sink: Optional[Sink] = None
