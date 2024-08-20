from __future__ import annotations

from typing import TYPE_CHECKING, Optional, cast

from attrs import define, field

from griptape.artifacts import TableArtifact
from griptape.loaders import BaseLoader
from griptape.utils import import_optional_dependency, str_to_hash

if TYPE_CHECKING:
    from pandas import DataFrame

    from griptape.drivers import BaseEmbeddingDriver


@define
class DataFrameLoader(BaseLoader):
    embedding_driver: Optional[BaseEmbeddingDriver] = field(default=None, kw_only=True)

    def load(self, source: DataFrame, *args, **kwargs) -> TableArtifact:
        artifact = TableArtifact(list(source.to_dict(orient="records")))

        if self.embedding_driver:
            artifact.generate_embedding(self.embedding_driver)

        return artifact

    def load_collection(self, sources: list[DataFrame], *args, **kwargs) -> dict[str, TableArtifact]:
        return cast(dict[str, TableArtifact], super().load_collection(sources, *args, **kwargs))

    def to_key(self, source: DataFrame, *args, **kwargs) -> str:
        hash_pandas_object = import_optional_dependency("pandas.core.util.hashing").hash_pandas_object

        return str_to_hash(str(hash_pandas_object(source, index=True).values))
