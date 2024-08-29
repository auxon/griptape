from __future__ import annotations

from attrs import define, field

from griptape.artifacts import TextArtifact
from griptape.loaders import BaseFileLoader


@define
class TextLoader(BaseFileLoader):
    encoding: str = field(default="utf-8", kw_only=True)

    def parse(self, source: bytes, *args, **kwargs) -> TextArtifact:
        return TextArtifact(source.decode(self.encoding), encoding=self.encoding)
