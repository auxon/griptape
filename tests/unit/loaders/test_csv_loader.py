import pytest

from griptape.loaders.csv_loader import CsvLoader
from tests.mocks.mock_embedding_driver import MockEmbeddingDriver


class TestCsvLoader:
    @pytest.fixture(params=["ascii", "utf-8", None])
    def loader(self, request):
        encoding = request.param
        if encoding is None:
            return CsvLoader(embedding_driver=MockEmbeddingDriver())
        else:
            return CsvLoader(encoding=encoding, embedding_driver=MockEmbeddingDriver())

    @pytest.fixture()
    def loader_with_pipe_delimiter(self):
        return CsvLoader(delimiter="|", embedding_driver=MockEmbeddingDriver())

    @pytest.fixture(params=["bytes_from_resource_path", "str_from_resource_path"])
    def create_source(self, request):
        return request.getfixturevalue(request.param)

    def test_load(self, loader, create_source):
        source = create_source("test-1.csv")

        artifact = loader.load(source)

        assert len(artifact) == 10
        first_artifact = artifact.value[0]
        assert first_artifact["Foo"] == "foo1"
        assert first_artifact["Bar"] == "bar1"
        assert artifact.embedding == [0, 1]

    def test_load_delimiter(self, loader_with_pipe_delimiter, create_source):
        source = create_source("test-pipe.csv")

        artifact = loader_with_pipe_delimiter.load(source)

        assert len(artifact) == 10
        first_artifact = artifact.value[0]
        assert first_artifact["Foo"] == "bar1"
        assert first_artifact["Bar"] == "foo1"
        assert artifact.embedding == [0, 1]

    def test_load_collection(self, loader, create_source):
        resource_paths = ["test-1.csv", "test-2.csv"]
        sources = [create_source(resource_path) for resource_path in resource_paths]

        collection = loader.load_collection(sources)

        keys = {loader.to_key(source) for source in sources}
        assert collection.keys() == keys

        for key in keys:
            artifact = collection[key]
            assert len(artifact) == 10
            first_artifact = artifact.value[0]
            assert first_artifact["Foo"] == "foo1"
            assert first_artifact["Bar"] == "bar1"
            assert artifact.embedding == [0, 1]

    def test_to_text(self, loader, create_source):
        source = create_source("test-1.csv")

        text = loader.load(source).to_text()

        assert (
            text
            == "Foo,Bar\nfoo1,bar1\nfoo2,bar2\nfoo3,bar3\nfoo4,bar4\nfoo5,bar5\nfoo6,bar6\nfoo7,bar7\nfoo8,bar8\nfoo9,bar9\nfoo10,bar10"
        )
