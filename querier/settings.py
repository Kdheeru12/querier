from pydantic import BaseSettings
from clickhouse_driver import connect


class Configs(BaseSettings):
    CLICKHOUSE_URL = "localhost:9002"
    DATABASE_NAME = "zen_traces_test"


settings = Configs()

DATABASES = {"clickhouse_local": {"host": "localhost", "port": "9002", "name": settings.DATABASE_NAME}}
