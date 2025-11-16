import json
import logging
import os
import uuid

from fastapi.encoders import jsonable_encoder

from domain import port

from etc.config import loader
env = loader.load_env(["./etc/config/root.local.yml"])
# Configuração do logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

class QueryWriter:
    @staticmethod
    def run_sql_in_file(
        conn: port.IStorageSession, filename: str, placeholder: list[str]
    ):
        path = os.path.join(env['app']['query_path'], filename)
        log.debug(f"reading: {path}")
        log.info(os.listdir())
        with open(path, "r") as filename:
            readed_file = filename.read()
            conn.execute(readed_file, placeholder)

    @staticmethod
    def run_sql_in_str(
        conn: port.IStorageSession, query: str, placeholder: list[any]
    ) -> list[tuple]:
        if placeholder is None:
            placeholder = []

        results = conn.execute(query, placeholder).fetchall()
        return results


class MoveRegistry:
    def __init__(self):
        self.writter = QueryWriter

    def insert_metric(
        self,
        conn,
        schema_fk: str,
        old_bucket: str,
        new_bucket: str,
        namespace: str,
        summary: str,
    ):
        self.writter.run_sql_in_str(
            conn,
            """
           insert into move_registry (schema_fk, old_bucket, new_bucket, namespace, summary) values (?,?,?,?,?)
        """,
            [schema_fk, old_bucket, new_bucket, namespace, summary],
        )

    def get_metrics(self, conn):
        rows = self.writter.run_sql_in_str(conn, "select * from metric", [])
        cols = self.writter.run_sql_in_str(conn, "describe metric", [])
        contents = [dict(zip(map(lambda x: x[0], cols), row)) for row in rows]

        contents = jsonable_encoder(contents)
        return contents


class SchemaRegistry:
    def __init__(self):
        self.writter = QueryWriter

    def get_avro_schema_by_namespace(self, conn: port.IStorageSession, namespace: str):
        rows = self.writter.run_sql_in_str(
            conn,
            "select id, schema_avro from schema_registry where namespace = ?",
            [namespace],
        )
        contents = [{"schema_avro": row[1], "id": row[0]} for row in rows]
        return contents

    def initialize_schema(
        self,
        conn: port.IStorageSession,
    ):
        self.writter.run_sql_in_file(conn, env['app']['migration'], [])

    def insert_schema(self, conn: port.IStorageSession, schema: str) -> str:
        uuid_str = str(uuid.uuid4())
        self.writter.run_sql_in_file(
            conn,
            "insert_schema.sql",
            [
                uuid_str,
                schema["namespace"],
                json.dumps(
                    schema,
                    ensure_ascii=False,
                ),
            ],
        )
        return uuid_str

    def delete_schema(
        self, conn: port.IStorageSession, namespace: str | None = None
    ) -> str:
        uuid_str = str(uuid.uuid4())
        if namespace:
            self.writter.run_sql_in_file(conn, "delete_schema_some.sql", [namespace])
            return uuid_str

        self.writter.run_sql_in_file(conn, "delete_schema_all.sql", [])

    def get_all(self, conn: port.IStorageSession):
        rows = self.writter.run_sql_in_str(conn, "select * from schema_registry", [])
        columns = self.writter.run_sql_in_str(conn, "DESCRIBE schema_registry", [])
        cols = [col[0] for col in columns]
        contents = [dict(zip(cols, row)) for row in rows]

        contents = jsonable_encoder(contents)
        return contents
