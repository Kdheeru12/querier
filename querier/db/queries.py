from db.clickhouseclient import ClickHouseBase
from settings import settings
from db.models import ServiceOverview, ServiceDependencyGraph, QueryKeys
from typing import List

database_name = settings.DATABASE_NAME
trace_table = "distributed_traces"
service_calls_table = "distributed_service_calls"
error_table = ""
dependency_table = "distributed_service_dependency_graph"
possible_query_key_table = "distributed_query_keys"


class ServiceCalls(ClickHouseBase):
    def get_distinct_service_names(self, service_filter=None) -> dict:
        query = f"SELECT DISTINCT name, serviceName FROM {database_name}.{service_calls_table}"
        if service_filter:
            query = query + f" Where serviceName = '{service_filter}' "
        print(query)
        rows = self.execute_query(query)
        services = dict()
        for url, servicename in rows:
            services.setdefault(servicename, []).append(url)
        return services


class TraceTable(ClickHouseBase):
    def p99_avg_duration_services(self, service_name, urls, start_time, end_time) -> ServiceOverview:
        query = f"""
            SELECT
                count(*) as numCalls,
                quantile(0.99)(durationNano) as p99,
                avg(durationNano) as avgDuration
            FROM {database_name}.{trace_table}
            WHERE serviceName = %(servicename)s AND name IN %(urls)s
                AND timestamp >= %(start_time)s AND timestamp <= %(end_time)s
        """
        params = {
            "servicename": service_name,
            "urls": urls,
            "start_time": start_time,
            "end_time": end_time,
        }
        results = self.execute_query(query, params)
        if results and results[0][0]:
            error_count = self.get_get_error_count_per_service(params)
            return ServiceOverview(
                service_name,
                results[0][0],
                results[0][1],
                results[0][2],
                error_count=error_count,
            )
        return None

    def p99_avg_duration_top_request_per_service(self, service_name, urls, start_time, end_time):
        query = f"""
            SELECT
                count(*) as numCalls,
                name as name,
                quantile(0.99)(durationNano) as p99,
                avg(durationNano) as avgDuration
            FROM {database_name}.{trace_table}
            WHERE serviceName = %(servicename)s AND name IN %(urls)s
                AND timestamp >= %(start_time)s AND timestamp <= %(end_time)s
            GROUP BY name
        """
        params = {
            "servicename": service_name,
            "urls": urls,
            "start_time": start_time,
            "end_time": end_time,
        }
        duration = (end_time - start_time).total_seconds()
        error_count_per_url = self.get_error_count_per_url_in_service(params)
        rows = self.execute_query(query, params)
        results = []
        for count, name, p99, avgduration in rows:
            results.append(
                ServiceOverview(
                    name,
                    count,
                    p99,
                    avgduration,
                    error_count_per_url.get(name, 0),
                )
            )

        return results

    def get_get_error_count_per_service(self, params):
        query = f"""
            SELECT
                count(*) as errorcount
            FROM {database_name}.{trace_table}
            WHERE serviceName = %(servicename)s AND name IN %(urls)s
                AND timestamp >= %(start_time)s AND timestamp <= %(end_time)s AND statusCode = 2
        """

        results = self.execute_query(query, params)
        return results[0][0]

    def get_error_count_per_url_in_service(self, params):
        query = f"""
            SELECT
                count(*) as errorcount,
                name as name
            FROM {database_name}.{trace_table}
            WHERE serviceName = %(servicename)s AND name IN %(urls)s
                AND timestamp >= %(start_time)s AND timestamp <= %(end_time)s AND statusCode = 2
            GROUP BY name
        """
        rows = self.execute_query(query, params)
        results = {}
        for count, name in rows:
            results[name] = count

        return results


a = "ddd"


class DependencyGraph(ClickHouseBase):
    def get_dependency_graph(self, params) -> List[ServiceDependencyGraph]:
        query = f"""
            WITH
                quantilesMergeState(0.5, 0.75, 0.9, 0.95, 0.99)(duration_quantiles_state) AS duration_quantiles_state,
                finalizeAggregation(duration_quantiles_state) AS result
            SELECT
                src as parent,
                dest as child,
                result[4] AS p95,
                result[5] AS p99,
                sum(total_count) as count,
                sum(error_count) as errorcount
            FROM {database_name}.{dependency_table}
            WHERE timestamp >= %(start_time)s AND timestamp <= %(end_time)s
            GROUP BY src, dest;
            """
        rows = self.execute_query(query, params)
        results = []
        for parent, child, p95, p99, count, error_count in rows:
            results.append(ServiceDependencyGraph(parent, child, p95, p99, count, error_count))
        return results


class QueryKeysToFilter(ClickHouseBase):
    def get_query_keys(self):
        query = (
            f"SELECT DISTINCT(tagKey), tagType, dataType, isColumn FROM {database_name}.{possible_query_key_table}"
        )
        rows = self.execute_query(query)
        results = []
        for tagKey, tagType, dataType, isColumn in rows:
            results.append(QueryKeys(tagKey, tagType, dataType, isColumn))
        return results


# class ErrorTable(ClickHouseBase):
#     def get_error_count_per_service(self, service_name, urls, start_time, end_time):
#         query = f"""
#             SELECT
#                 count(*) as errcount,
#             FROM {database_name}.{trace_table}
#             WHERE serviceName = %(servicename)s AND name IN %(urls)s
#                 AND timestamp >= %(start_time)s AND timestamp <= %(end_time)s
#         """
