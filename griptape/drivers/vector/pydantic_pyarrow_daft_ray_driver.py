from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, List, Dict

import ray
import pyarrow as pa
from pydantic import BaseModel, ValidationError
import daft
from attrs import define, field

from griptape import utils
from griptape.artifacts import BaseArtifact, ListArtifact, TextArtifact
from griptape.mixins import FuturesExecutorMixin, SerializableMixin

if TYPE_CHECKING:
    from griptape.drivers import BaseEmbeddingDriver


# Initialize Ray
ray.init(ignore_reinit_error=True)


# Example Pydantic model for validation
class VectorMetadataModel(BaseModel):
    id: str
    namespace: Optional[str]
    score: Optional[float]
    meta: Optional[Dict[str, Any]]


@define
class PydanticPyArrowDaftRayDriver(SerializableMixin, FuturesExecutorMixin, ABC):
    DEFAULT_QUERY_COUNT = 5

    @dataclass
    class Entry:
        id: str
        vector: Optional[List[float]] = None
        score: Optional[float] = None
        meta: Optional[Dict[str, Any]] = None
        namespace: Optional[str] = None

        @staticmethod
        def from_dict(data: Dict[str, Any]) -> PydanticPyArrowDaftRayDriver.Entry:
            return PydanticPyArrowDaftRayDriver.Entry(**data)

        def to_artifact(self) -> BaseArtifact:
            return BaseArtifact.from_json(self.meta["artifact"])  # pyright: ignore[reportOptionalSubscript]

    embedding_driver: BaseEmbeddingDriver = field(kw_only=True, metadata={"serializable": True})

    def validate_metadata(self, meta: Dict[str, Any]):
        """
        Validate metadata using Pydantic.
        """
        try:
            validated_meta = VectorMetadataModel(**meta)
            return validated_meta
        except ValidationError as e:
            raise ValueError(f"Metadata validation error: {e}")

    def upsert_text_artifacts(
        self,
        artifacts: List[TextArtifact] | Dict[str, List[TextArtifact]],
        *,
        meta: Optional[Dict] = None,
        **kwargs,
    ) -> None:
        if isinstance(artifacts, list):
            utils.execute_futures_list(
                [
                    self.futures_executor.submit(self.upsert_text_artifact, a, namespace=None, meta=meta, **kwargs)
                    for a in artifacts
                ],
            )
        else:
            futures_dict = {}

            for namespace, artifact_list in artifacts.items():
                for a in artifact_list:
                    if not futures_dict.get(namespace):
                        futures_dict[namespace] = []

                    futures_dict[namespace].append(
                        self.futures_executor.submit(
                            self.upsert_text_artifact, a, namespace=namespace, meta=meta, **kwargs
                        )
                    )

            utils.execute_futures_list_dict(futures_dict)

    def upsert_text_artifact(
        self,
        artifact: TextArtifact,
        *,
        namespace: Optional[str] = None,
        meta: Optional[Dict] = None,
        vector_id: Optional[str] = None,
        **kwargs,
    ) -> str:
        meta = {} if meta is None else self.validate_metadata(meta).dict()

        if vector_id is None:
            value = artifact.to_text() if artifact.reference is None else artifact.to_text() + str(artifact.reference)
            vector_id = self._get_default_vector_id(value)

        if self.does_entry_exist(vector_id, namespace=namespace):
            return vector_id
        else:
            meta["artifact"] = artifact.to_json()

            vector = artifact.embedding or artifact.generate_embedding(self.embedding_driver)

            if isinstance(vector, list):
                return self.upsert_vector(vector, vector_id=vector_id, namespace=namespace, meta=meta, **kwargs)
            else:
                raise ValueError("Vector must be an instance of 'list'.")

    def upsert_text(
        self,
        string: str,
        *,
        vector_id: Optional[str] = None,
        namespace: Optional[str] = None,
        meta: Optional[Dict] = None,
        **kwargs,
    ) -> str:
        vector_id = self._get_default_vector_id(string) if vector_id is None else vector_id

        if self.does_entry_exist(vector_id, namespace=namespace):
            return vector_id
        else:
            # Validate metadata before upserting
            meta = self.validate_metadata(meta or {}).dict()
            return self.upsert_vector(
                self.embedding_driver.embed_string(string),
                vector_id=vector_id,
                namespace=namespace,
                meta=meta,
                **kwargs,
            )

    def does_entry_exist(self, vector_id: str, *, namespace: Optional[str] = None) -> bool:
        try:
            return self.load_entry(vector_id, namespace=namespace) is not None
        except Exception:
            return False

    def load_artifacts(self, *, namespace: Optional[str] = None) -> ListArtifact:
        result = self.load_entries(namespace=namespace)
        artifacts = [r.to_artifact() for r in result]

        return ListArtifact([a for a in artifacts if isinstance(a, TextArtifact)])

    def to_pyarrow_table(self, entries: List[Entry]) -> pa.Table:
        """
        Convert list of entries to a PyArrow Table for efficient processing.
        """
        data = {
            "id": [entry.id for entry in entries],
            "vector": [entry.vector for entry in entries],
            "score": [entry.score for entry in entries],
            "namespace": [entry.namespace for entry in entries],
        }
        return pa.table(data)

    @ray.remote
    def process_with_daft(self, arrow_table: pa.Table) -> daft.DataFrame:
        """
        Distributed processing with Daft using Ray.
        """
        daft_df = daft.from_arrow(arrow_table)
        # Example: Add a new column with transformed data
        processed_df = daft_df.with_column("processed_score", daft_df["score"] * 2)
        return processed_df

    def distributed_process(self, entries: List[Entry]):
        """
        Convert entries to PyArrow Table, process with Daft and Ray.
        """
        arrow_table = self.to_pyarrow_table(entries)
        result_df = ray.get(self.process_with_daft.remote(arrow_table))
        return result_df

    @abstractmethod
    def delete_vector(self, vector_id: str) -> None:
        ...

    @abstractmethod
    def upsert_vector(
        self,
        vector: List[float],
        *,
        vector_id: Optional[str] = None,
        namespace: Optional[str] = None,
        meta: Optional[Dict] = None,
        **kwargs,
    ) -> str:
        ...

    @abstractmethod
    def load_entry(self, vector_id: str, *, namespace: Optional[str] = None) -> Optional[Entry]:
        ...

    @abstractmethod
    def load_entries(self, *, namespace: Optional[str] = None) -> List[Entry]:
        ...

    @abstractmethod
    def query(
        self,
        query: str,
        *,
        count: Optional[int] = None,
        namespace: Optional[str] = None,
        include_vectors: bool = False,
        **kwargs,
    ) -> List[Entry]:
        ...

    def _get_default_vector_id(self, value: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_OID, value))
