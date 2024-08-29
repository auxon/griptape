from __future__ import annotations

from typing import TYPE_CHECKING

from attrs import define, field

from griptape.artifacts import TableArtifact
from griptape.loaders import BaseLoader

if TYPE_CHECKING:
    from griptape.drivers import BaseSqlDriver


@define
class SqlLoader(BaseLoader):
    sql_driver: BaseSqlDriver = field(kw_only=True)

    def load(self, source: str, *args, **kwargs) -> TableArtifact:
        rows = self.sql_driver.execute_query(source)
        artifact = TableArtifact([row.cells for row in rows] if rows else [])

        return artifact
