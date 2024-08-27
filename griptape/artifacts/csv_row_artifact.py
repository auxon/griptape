from __future__ import annotations

import csv
import io
import json
from typing import Any

from attrs import Converter, define, field

from griptape.artifacts import TextArtifact


@define
class CsvRowArtifact(TextArtifact):
    value: dict[str, str] = field(
        converter=Converter(lambda value: CsvRowArtifact.value_to_dict(value)), metadata={"serializable": True}
    )
    delimiter: str = field(default=",", kw_only=True, metadata={"serializable": True})

    def __bool__(self) -> bool:
        return len(self) > 0

    @classmethod
    def value_to_dict(cls, value: Any) -> dict:
        dict_value = value if isinstance(value, dict) else json.loads(value)

        return dict(dict_value.items())

    def to_text(self) -> str:
        with io.StringIO() as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=self.value.keys(),
                quoting=csv.QUOTE_MINIMAL,
                delimiter=self.delimiter,
            )

            writer.writeheader()
            writer.writerow(self.value)

            return csvfile.getvalue().strip()
