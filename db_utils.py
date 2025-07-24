import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Self

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine import URL, Connection, Engine
from sqlmodel import create_engine


class RedshiftPostgresDialect(PGDialect_psycopg2):
    def _set_backslash_escapes(self, connection: Connection):
        self._backslash_escapes = "off"


registry.register("redshift_custom", __name__, "RedshiftPostgresDialect")


class RedshiftDB:
    _instance: Self | None = None
    _executor: ThreadPoolExecutor | None = None

    def __init__(self, engine: Engine, host: str):
        if RedshiftDB._instance is not None:
            raise RuntimeError("RedshiftDB is a singleton class, use connect() method.")
        self.engine = engine
        self.host = host
        if RedshiftDB._executor is None:
            RedshiftDB._executor = ThreadPoolExecutor(max_workers=3)

    @classmethod
    def connect(cls) -> Self:
        if cls._instance is not None:
            return cls._instance
        redshift_host = os.environ["REDSHIFT_HOST"]
        redshift_url = URL.create(
            drivername="redshift_custom",
            username=os.environ["REDSHIFT_USER"],
            password=os.environ["REDSHIFT_PASSWORD"],
            database=os.environ["REDSHIFT_DB"],
            host=os.environ["REDSHIFT_HOST"],
            port=5439,
            query={"sslmode": "require"},
        )
        engine = create_engine(redshift_url)
        cls._instance = cls(engine, redshift_host)
        return cls._instance

    def disconnect(self):
        self.engine.dispose()
        if RedshiftDB._executor:
            RedshiftDB._executor.shutdown(wait=True)
            RedshiftDB._executor = None

    def _run_query_sync(self, sql: str):
        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = result.fetchall()
            columns = result.keys()
            return pd.DataFrame(rows, columns=columns)  # type: ignore

    async def run_query(self, sql: str):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, self._run_query_sync, sql)
