from __future__ import annotations

from attrs import define

from griptape.artifacts import AudioArtifact
from griptape.loaders.base_file_loader import BaseFileLoader
from griptape.utils import import_optional_dependency


@define
class AudioLoader(BaseFileLoader):
    """Loads audio content into audio artifacts."""

    def parse(self, source: bytes, *args, **kwargs) -> AudioArtifact:
        filetype = import_optional_dependency("filetype")

        return AudioArtifact(source, format=filetype.guess(source).extension)
