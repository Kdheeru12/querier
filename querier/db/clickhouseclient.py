from clickhouse_driver import Client
from settings import DATABASES

database = DATABASES["clickhouse_local"]


class QueryExecutionError(Exception):
    pass


class ClickHouseBase:
    def __init__(self, host=database["host"], port=database["port"], database=database["name"]):
        self.host = host
        self.port = port
        self.database = database
        print(self.host, self.port, self.database)
        self.client = Client(host=self.host, port=self.port, database=self.database)

    def execute_query(
        self,
        query,
        params=None,
        with_column_types=False,
        external_tables=None,
        query_id=None,
        settings=None,
        types_check=False,
        columnar=False,
    ):
        try:
            result = self.client.execute(
                query, params, with_column_types, external_tables, query_id, settings, types_check, columnar
            )
        except Exception as e:
            error_msg = "Query execution failed: " + str(e)
            raise QueryExecutionError(error_msg)
        return result


# client = Client(host=database["host"], port=database["port"], database=database["name"])

# client.execute("show tables")
