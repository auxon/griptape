from __future__ import annotations

from io import BytesIO
from typing import Optional

from attrs import define

from griptape.artifacts import ListArtifact, TextArtifact
from griptape.loaders import BaseLoader
from griptape.utils import import_optional_dependency


@define
class PdfLoader(BaseLoader):
    def parse(
        self,
        source: bytes,
        password: Optional[str] = None,
        *args,
        **kwargs,
    ) -> ListArtifact:
        pypdf = import_optional_dependency("pypdf")

        reader = pypdf.PdfReader(BytesIO(source), strict=True, password=password)
        pages = [TextArtifact(p.extract_text()) for p in reader.pages]

        return ListArtifact(pages)
