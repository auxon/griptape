from __future__ import annotations

from typing import cast

from attrs import define, field

from griptape.artifacts import TextArtifact
from griptape.loaders import BaseTextLoader


@define
class TextLoader(BaseTextLoader):
    encoding: str = field(default="utf-8", kw_only=True)

    def load(self, source: str | bytes, *args, **kwargs) -> TextArtifact:
        if isinstance(source, bytes):
            source = source.decode(encoding=self.encoding)
        elif isinstance(source, (bytearray, memoryview)):
            raise ValueError(f"Unsupported source type: {type(source)}")

        return TextArtifact(source)

    def load_collection(
        self,
        sources: list[bytes | str],
        *args,
        **kwargs,
    ) -> dict[str, TextArtifact]:
        return cast(
            dict[str, TextArtifact],
            super().load_collection(sources, *args, **kwargs),
        )
