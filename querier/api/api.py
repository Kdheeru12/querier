from db.queries import ServiceCalls, TraceTable, DependencyGraph, QueryKeysToFilter, service_query, trace_table
import datetime


def getservices():
    services = service_query.get_distinct_service_names()
    trace_table = TraceTable()
    start_time = datetime.datetime.now() - datetime.timedelta(days=10)
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    data = []
    for service, urls in services.items():
        res = trace_table.p99_avg_duration_services(service, urls, start_time, end_time)
        if res:
            res.timeframe = duration
            data.append(res)
    print(data)


def gettopurlsofservice(service_name=None):
    services = service_query.get_distinct_service_names(service_name)
    print(services)
    start_time = datetime.datetime.now() - datetime.timedelta(days=10)
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    results = {}
    for service_name, urls in services.items():
        trace_table = TraceTable()
        data = trace_table.p99_avg_duration_top_request_per_service(service_name, urls, start_time, end_time)
        results.setdefault(service_name, []).extend(data)
    print(results)


def get_service_dependency_graph():
    start_time = datetime.datetime.now() - datetime.timedelta(days=10)
    end_time = datetime.datetime.now()
    params = {
        "start_time": start_time,
        "end_time": end_time,
    }
    graph = DependencyGraph().get_dependency_graph(params)
    print(graph)


def get_query_keys():
    data = QueryKeysToFilter().get_query_keys()
    return data


def get_anomalous_requests(service_name):
    services = service_query.get_distinct_service_names(service_name)
    services_to_query = []
    endpoints = []
    for service_name, urls in services.items():
        services_to_query.append(service_name)
        endpoints.extend(urls)
    params = {"servicenames": list(set(services_to_query)), "urls": list(set(endpoints))}
    return trace_table.get_slow_queries_by_service(params)


def analyse_slow_trace(traceID):
    pass
