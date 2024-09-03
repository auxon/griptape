
import json
from typing import List, Dict, Optional
import uuid
import lancedb
import pyarrow as pa

from attrs import define, field
from griptape.mixins import SerializableMixin, FuturesExecutorMixin
from griptape.drivers.vector.pydantic_pyarrow_daft_ray_driver import PydanticPyArrowDaftRayDriver
from griptape.drivers.embedding.base_embedding_driver import BaseEmbeddingDriver

@define
class PydanticPyArrowDaftRayLanceDBDriver(SerializableMixin, FuturesExecutorMixin):
    """
    LanceDB driver for Griptape using PyArrow and Ray.
    """
    embedding_driver: BaseEmbeddingDriver = field(kw_only=True, metadata={"serializable": True})
    lancedb_path: str = field(kw_only=True, default="lancedb_dir", metadata={"serializable": True})
    DEFAULT_QUERY_COUNT: int = 10


    lancedb: "lancedb.LanceDBConnection" = field(init=False)
    table_name: str = field(default="vectors", init=False)

    def __attrs_post_init__(self):
        self.lancedb = lancedb.connect(self.lancedb_path)
        if self.table_name not in self.lancedb.table_names():
            self.lancedb.create_table(self.table_name, schema=self.create_schema())


    def create_schema(self):
        """
        Define the schema for the LanceDB table.
        """
        return pa.schema([
            ("id", pa.string()),
            ("vector", pa.list_(pa.float32())),
            ("namespace", pa.string()),
            ("meta", pa.string()),  # Store metadata as JSON string
            ("score", pa.float32())
        ])

    def upsert_vector(
        self,
        vector: List[float],
        *,
        vector_id: Optional[str] = None,
        namespace: Optional[str] = None,
        meta: Optional[Dict] = None,
        **kwargs,
    ) -> str:
        """
        Insert or update a vector entry in LanceDB.
        """
        if vector_id is None:
            vector_id = self._get_default_vector_id(str(vector))

        table = self.lancedb.open_table(self.table_name)
        data = {
            "id": vector_id,
            "vector": vector,
            "namespace": namespace or "",
            "meta": json.dumps(meta or {}),
            "score": kwargs.get("score", 0.0)
        }

        # Upsert the vector in LanceDB
        table.add(pa.table([data]), mode="overwrite")

        return vector_id

    def load_entry(self, vector_id: str, *, namespace: Optional[str] = None) -> Optional[PydanticPyArrowDaftRayDriver.Entry]:
        """
        Load a single entry from LanceDB by vector ID.
        """
        table = self.lancedb.open_table(self.table_name)
        query = table.search(f"id == '{vector_id}'")

        if namespace:
            query = query.where(f"namespace == '{namespace}'")

        result = query.to_pandas().to_dict(orient="records")
        # ... rest of the method
        if result:
            return PydanticPyArrowDaftRayDriver.Entry(**result[0])
        return None

    def load_entries(self, *, namespace: Optional[str] = None) -> List[PydanticPyArrowDaftRayDriver.Entry]:
        """
        Load all entries from LanceDB, optionally filtered by namespace.
        """
        table = self.lancedb.open_table(self.table_name)

        if namespace:
            results = table.search(f"namespace == '{namespace}'").to_pandas().to_dict(orient="records")
        else:
            results = table.to_pandas().to_dict(orient="records")

        return [PydanticPyArrowDaftRayDriver.Entry(**r) for r in results]

    def delete_vector(self, vector_id: str) -> None:
        """
        Delete a vector from LanceDB by vector ID.
        """
        table = self.lancedb.open_table(self.table_name)
        table.delete(f"id == '{vector_id}'")

    def query(
        self,
        query_vector: List[float],
        *,
        count: Optional[int] = None,
        namespace: Optional[str] = None,
    ) -> List[PydanticPyArrowDaftRayDriver.Entry]:
        """
        Query LanceDB for similar vectors.
        """
        table = self.lancedb.open_table(self.table_name)
        query = table.search(query_vector)

        if namespace:
            query = query.where(f"namespace == '{namespace}'")

        # Limit the number of results returned
        query = query.limit(count or self.DEFAULT_QUERY_COUNT)

        # Get results as a list of entries
        results = query.to_pandas().to_dict(orient="records")

        return [PydanticPyArrowDaftRayDriver.Entry(**r) for r in results]

    def _get_default_vector_id(self, value: str) -> str:
        """
        Generate a UUID for a vector based on its value.
        """
        return str(uuid.uuid5(uuid.NAMESPACE_OID, value))
