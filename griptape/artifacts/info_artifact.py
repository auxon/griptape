from __future__ import annotations

from attrs import define, field

from griptape.artifacts import BaseSystemArtifact


@define
class InfoArtifact(BaseSystemArtifact):
    value: str = field(converter=str, metadata={"serializable": True})
