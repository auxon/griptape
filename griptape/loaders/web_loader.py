from __future__ import annotations

from typing import TYPE_CHECKING

from attrs import Factory, define, field

from griptape.drivers import BaseWebScraperDriver, TrafilaturaWebScraperDriver
from griptape.loaders import BaseTextLoader

if TYPE_CHECKING:
    from griptape.artifacts import TextArtifact


@define
class WebLoader(BaseTextLoader):
    web_scraper_driver: BaseWebScraperDriver = field(
        default=Factory(lambda: TrafilaturaWebScraperDriver()),
        kw_only=True,
    )

    def load(self, source: str, *args, **kwargs) -> TextArtifact:
        return self.web_scraper_driver.scrape_url(source)
