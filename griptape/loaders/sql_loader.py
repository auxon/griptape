from __future__ import annotations

from typing import TYPE_CHECKING, Optional, cast

from attrs import define, field

from griptape.artifacts import TableArtifact
from griptape.loaders import BaseLoader

if TYPE_CHECKING:
    from griptape.drivers import BaseEmbeddingDriver, BaseSqlDriver


@define
class SqlLoader(BaseLoader):
    sql_driver: BaseSqlDriver = field(kw_only=True)
    embedding_driver: Optional[BaseEmbeddingDriver] = field(default=None, kw_only=True)

    def load(self, source: str, *args, **kwargs) -> TableArtifact:
        rows = self.sql_driver.execute_query(source)
        artifact = TableArtifact([row.cells for row in rows] if rows else [])

        if self.embedding_driver:
            artifact.generate_embedding(self.embedding_driver)

        return artifact

    def load_collection(self, sources: list[str], *args, **kwargs) -> dict[str, TableArtifact]:
        return cast(dict[str, TableArtifact], super().load_collection(sources, *args, **kwargs))
