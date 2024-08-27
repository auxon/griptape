from abc import ABC

from griptape.artifacts import BaseArtifact


class BaseSystemArtifact(BaseArtifact, ABC):
    """Base class for Artifacts specific to Griptape."""

    def to_text(self) -> str:
        return self.value
