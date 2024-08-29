from __future__ import annotations

from abc import ABC
from io import BytesIO
from os import PathLike
from pathlib import Path
from typing import Optional

from attrs import define, field

from griptape.loaders import BaseLoader


@define
class BaseFileLoader(BaseLoader, ABC):
    encoding: Optional[str] = field(default=None, kw_only=True)

    def fetch(self, source: str | BytesIO | PathLike, *args, **kwargs) -> bytes:
        if isinstance(source, (str, PathLike)):
            content = Path(source).read_bytes()
        elif isinstance(source, BytesIO):
            content = source.read()

        return content
