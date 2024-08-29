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

    def fetch(self, source: str, *args, **kwargs) -> list[BaseSqlDriver.RowResult]:
        return self.sql_driver.execute_query(source) or []

    def parse(self, source: list[BaseSqlDriver.RowResult], *args, **kwargs) -> TableArtifact:
        return TableArtifact([row.cells for row in source])
