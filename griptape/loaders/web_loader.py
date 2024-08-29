from __future__ import annotations

from attrs import Factory, define, field

from griptape.artifacts import TextArtifact
from griptape.drivers import BaseWebScraperDriver, TrafilaturaWebScraperDriver
from griptape.loaders import BaseLoader


@define
class WebLoader(BaseLoader):
    web_scraper_driver: BaseWebScraperDriver = field(
        default=Factory(lambda: TrafilaturaWebScraperDriver()),
        kw_only=True,
    )

    def fetch(self, source: str, *args, **kwargs) -> bytes:
        return self.web_scraper_driver.scrape_url(source).value.encode()

    def parse(self, source: bytes, *args, **kwargs) -> TextArtifact:
        return TextArtifact(source.decode())
