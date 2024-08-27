from __future__ import annotations

import base64

from attrs import define, field

from griptape.artifacts import BaseArtifact


@define
class ImageArtifact(BaseArtifact):
    """ImageArtifact is a type of Artifact representing an image.

    Attributes:
        value: Raw bytes representing media data.
        format: The format of the media, like png, jpeg, or gif. Default is png.
        width: The width of the image in pixels.
        height: The height of the image in pixels.
    """

    value: bytes = field(metadata={"serializable": True})
    format: str = field(default="png", kw_only=True, metadata={"serializable": True})
    width: int = field(kw_only=True, metadata={"serializable": True})
    height: int = field(kw_only=True, metadata={"serializable": True})

    @property
    def base64(self) -> str:
        return base64.b64encode(self.value).decode("utf-8")

    @property
    def mime_type(self) -> str:
        return f"image/{self.format}"

    def to_text(self) -> str:
        return self.base64
