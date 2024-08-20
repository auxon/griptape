from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, cast

from attrs import Factory, define, field

from griptape.artifacts import TextArtifact
from griptape.loaders import BaseLoader
from griptape.tokenizers import OpenAiTokenizer

if TYPE_CHECKING:
    from griptape.common import Reference
    from griptape.tokenizers import BaseTokenizer


@define
class BaseTextLoader(BaseLoader, ABC):
    tokenizer: BaseTokenizer = field(
        default=Factory(lambda: OpenAiTokenizer(model=OpenAiTokenizer.DEFAULT_OPENAI_GPT_3_CHAT_MODEL)),
        kw_only=True,
    )
    encoding: str = field(default="utf-8", kw_only=True)
    reference: Optional[Reference] = field(default=None, kw_only=True)

    @abstractmethod
    def load(self, source: Any, *args, **kwargs) -> TextArtifact: ...

    def load_collection(self, sources: list[Any], *args, **kwargs) -> dict[str, TextArtifact]:
        return cast(
            dict[str, TextArtifact],
            super().load_collection(sources, *args, **kwargs),
        )
