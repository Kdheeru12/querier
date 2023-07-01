from db.queries import ServiceCalls, TraceTable, DependencyGraph, QueryKeysToFilter
import datetime


def getservices():
    services = ServiceCalls().get_distinct_service_names()
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


def gettopurlsofservice(service_name):
    services = ServiceCalls().get_distinct_service_names(service_name)
    print(services)
    start_time = datetime.datetime.now() - datetime.timedelta(days=10)
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    if services[service_name]:
        trace_table = TraceTable()
        data = trace_table.p99_avg_duration_top_request_per_service(
            service_name, services[service_name], start_time, end_time
        )
        print(data)


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
