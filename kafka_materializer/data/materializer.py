from typing import Any, Dict, List, Optional, Tuple
import orjson
import cassandra.query
import hashlib
import threading
from kafka_materializer.settings import Settings
from cassandra.cluster import Cluster
from cassandra.cluster import PreparedStatement
from ..types import View

TABLES = [
    """
    CREATE TABLE IF NOT EXISTS {keyspace}.views (
        id text,
        data text,
        PRIMARY KEY (id)
    );
""",
    """
    CREATE TABLE IF NOT EXISTS {keyspace}.materialized_data (
        view_id text,
        order_by text,
        data text,
        PRIMARY KEY ((view_id), order_by)
    );
""",
]


class CassandraResultItem:
    id: str
    data: str


class CassandraResult:
    current_rows: List[CassandraResultItem]


class MaterializerDataService:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._prepared_registry: Dict[str, PreparedStatement] = {}
        self._prepare_lock = threading.Lock()
        self._cluster = Cluster(
            contact_points=self._settings.cassandra_servers,
            protocol_version=4,
        )
        self._session = self._cluster.connect()
        self._session.default_fetch_size = 50

    def bootstrap(self):
        self._execute(
            """
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH replication = {replication};
""",
            replication=self._settings.cassandra_replication,
        )
        for table in TABLES:
            self._execute(table)

    def close(self):
        self._cluster.shutdown()

    def prepare(self, query: str) -> PreparedStatement:
        hashed = hashlib.md5(query.encode())
        if prepared := self._prepared_registry.get(hashed.hexdigest()):
            return prepared

        with self._prepare_lock:
            if prepared := self._prepared_registry.get(hashed.hexdigest()):
                return prepared

            # This method make a request to cassandra to bring back the prepared query id
            prepared = self._session.prepare(query)
            self._prepared_registry[hashed.hexdigest()] = prepared
            return prepared

    def _execute(
        self,
        query: str,
        params: Dict[str, Any] = None,
        page_size: int = None,
        paging_state: bytes = None,
        **cql_vars,
    ) -> CassandraResult:
        if not params:
            statement = cassandra.query.SimpleStatement(
                query.format(keyspace=self._settings.cassandra_keyspace, **cql_vars)
            )
        else:
            prepared = self.prepare(
                query.format(keyspace=self._settings.cassandra_keyspace)
            )
            statement = prepared.bind(params)

        if page_size:
            statement.fetch_size = page_size

        return self._session.execute(statement, paging_state=paging_state)

    def create_view(self, view: View) -> None:
        self._execute(
            "INSERT INTO {keyspace}.views (id, data) VALUES (?, ?)",
            {"id": view.id, "data": view.json()},
        )

    def delete_view(self, view_id: str) -> None:
        self._execute("DELETE FROM {keyspace}.views WHERE id = ?", {"id": view_id})

    def get_views(
        self, cursor: Optional[str] = None, page_size: int = 20
    ) -> Tuple[Optional[str], List[View]]:
        if cursor is None:
            results = self._execute(
                """SELECT id, data FROM {keyspace}.views""", page_size=page_size + 1
            )
        else:
            results = self._execute(
                """SELECT id, data FROM {keyspace}.views WHERE id >= ?""",
                {"id": cursor},
                page_size=page_size + 1,
            )

        result_cursor = None
        if len(results.current_rows) > page_size:
            result_cursor = results.current_rows[-1].id

        data = []
        for item in results.current_rows[:page_size]:
            data.append(View.parse_raw(item.data))

        return result_cursor, data

    def get_view(self, view_id: str) -> View:
        results = self._execute(
            """SELECT id, data FROM {keyspace}.views where id = ?""",
            params={"id": view_id},
            page_size=1,
        )
        return View.parse_raw(results.current_rows[0].data)

    def create_material(self, view_id: str, order: str, data: str) -> None:
        self._execute(
            "INSERT INTO {keyspace}.materialized_data (view_id, order_by, data) VALUES (?, ?, ?)",
            {"view_id": view_id, "order_by": order, "data": data},
        )

    def query_view(
        self,
        view_id: str,
        cursor: Optional[str] = None,
        page_size: int = 20,
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        view = self.get_view(view_id)
        params = {"view_id": view_id}

        if cursor is not None:
            query = """
SELECT order_by, data FROM {keyspace}.materialized_data
where view_id = ? AND order_by >= ?
"""
            params["order_by"] = cursor
        else:
            query = """
SELECT order_by, data FROM {keyspace}.materialized_data
where view_id = ?
"""

        results = self._execute(query, params=params, page_size=page_size + 1)
        result_cursor = None
        if len(results.current_rows) > page_size:
            result_cursor = results.current_rows[-1].order_by

        data = []
        for item in results.current_rows[:page_size]:
            idata = orjson.loads(item.data)
            idata[view.order_by] = item.order_by
            data.append(idata)

        return result_cursor, data
