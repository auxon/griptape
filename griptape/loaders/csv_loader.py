from __future__ import annotations

import csv

from attrs import define, field

from griptape.artifacts import TableArtifact
from griptape.loaders.text_loader import TextLoader


@define
class CsvLoader(TextLoader):
    delimiter: str = field(default=",", kw_only=True)
    encoding: str = field(default="utf-8", kw_only=True)

    def parse(self, source: bytes, *args, **kwargs) -> TableArtifact:
        reader = csv.DictReader(source.decode(self.encoding), delimiter=self.delimiter)

        return TableArtifact(list(reader), delimiter=self.delimiter, fieldnames=reader.fieldnames)
