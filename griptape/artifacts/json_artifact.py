from __future__ import annotations

import json
from typing import Any, Union

from attrs import Converter, define, field

from griptape.artifacts.text_artifact import TextArtifact


@define
class JsonArtifact(TextArtifact):
    Json = Union[dict[str, "Json"], list["Json"], str, int, float, bool, None]

    value: Json = field(
        converter=Converter(lambda value: JsonArtifact.value_to_dict(value)), metadata={"serializable": True}
    )

    @classmethod
    def value_to_dict(cls, value: Any) -> dict:
        return json.loads(json.dumps(value))

    def to_text(self) -> str:
        return json.dumps(self.value)
