from __future__ import annotations

from typing import Optional

from attrs import define, field

from griptape.artifacts import BaseSystemArtifact


@define
class ErrorArtifact(BaseSystemArtifact):
    value: str = field(converter=str, metadata={"serializable": True})
    exception: Optional[Exception] = field(default=None, kw_only=True, metadata={"serializable": False})
