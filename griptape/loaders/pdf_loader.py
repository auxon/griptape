from __future__ import annotations

from io import BytesIO
from typing import Optional, cast

from attrs import Factory, define, field

from griptape.artifacts import ListArtifact
from griptape.chunkers import PdfChunker
from griptape.loaders import BaseTextLoader
from griptape.utils import import_optional_dependency


@define
class PdfLoader(BaseTextLoader):
    chunker: PdfChunker = field(
        default=Factory(lambda self: PdfChunker(tokenizer=self.tokenizer, max_tokens=self.max_tokens), takes_self=True),
        kw_only=True,
    )
    encoding: None = field(default=None, kw_only=True)

    def load(
        self,
        source: bytes,
        password: Optional[str] = None,
        *args,
        **kwargs,
    ) -> ListArtifact:
        pypdf = import_optional_dependency("pypdf")
        reader = pypdf.PdfReader(BytesIO(source), strict=True, password=password)

        return ListArtifact([p.extract_text() for p in reader.pages])

    def load_collection(self, sources: list[bytes], *args, **kwargs) -> dict[str, ListArtifact]:
        return cast(
            dict[str, ListArtifact],
            super().load_collection(sources, *args, **kwargs),
        )
