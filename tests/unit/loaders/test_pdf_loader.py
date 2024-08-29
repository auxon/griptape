import pytest

from griptape.loaders import PdfLoader


class TestPdfLoader:
    @pytest.fixture()
    def loader(self):
        return PdfLoader()

    @pytest.fixture()
    def create_source(self, bytes_from_resource_path):
        return bytes_from_resource_path

    def test_load(self, loader, create_source):
        source = create_source("bitcoin.pdf")

        artifact = loader.load(source)

        assert len(artifact) == 9
        assert artifact.value.startswith("Bitcoin: A Peer-to-Peer")
        assert artifact.value.endswith('its applications," 1957.\n9')

    def test_load_collection(self, loader, create_source):
        resource_paths = ["bitcoin.pdf", "bitcoin-2.pdf"]
        sources = [create_source(resource_path) for resource_path in resource_paths]

        collection = loader.load_collection(sources)

        keys = {loader.to_key(source) for source in sources}

        assert collection.keys() == keys

        for key in keys:
            artifact = collection[key]
            assert len(artifact) == 9
            assert artifact.value.startswith("Bitcoin: A Peer-to-Peer")
            assert artifact.value.endswith('its applications," 1957.\n9')
