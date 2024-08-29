from __future__ import annotations

from attrs import define

from griptape.artifacts import AudioArtifact
from griptape.loaders import BaseLoader
from griptape.utils import import_optional_dependency


@define
class AudioLoader(BaseLoader):
    """Loads audio content into audio artifacts."""

    def load(self, source: bytes, *args, **kwargs) -> AudioArtifact:
        filetype = import_optional_dependency("filetype")

        return AudioArtifact(source, format=filetype.guess(source).extension)
