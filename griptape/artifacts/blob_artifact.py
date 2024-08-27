from __future__ import annotations

from typing import Any

from attrs import Converter, define, field

from griptape.artifacts import BaseArtifact


@define
class BlobArtifact(BaseArtifact):
    value: bytes = field(
        converter=Converter(lambda value: BlobArtifact.value_to_bytes(value)),
        metadata={"serializable": True},
    )
    encoding: str = field(default="utf-8", kw_only=True)
    encoding_error_handler: str = field(default="strict", kw_only=True)
    media_type: str = field(default="application/octet-stream", kw_only=True)

    @classmethod
    def value_to_bytes(cls, value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        else:
            return str(value).encode()

    def to_text(self) -> str:
        return self.value.decode(encoding=self.encoding, errors=self.encoding_error_handler)
