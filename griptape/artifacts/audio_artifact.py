from __future__ import annotations

from attrs import define, field

from griptape.artifacts import BaseArtifact


@define
class AudioArtifact(BaseArtifact):
    """AudioArtifact is a type of Artifact representing audio."""

    value: bytes = field(metadata={"serializable": True})
    format: str = field(kw_only=True, metadata={"serializable": True})

    @property
    def mime_type(self) -> str:
        return f"audio/{self.format}"

    def to_text(self) -> str:
        return f"Audio, format: {self.format}, size: {len(self.value)} bytes"
