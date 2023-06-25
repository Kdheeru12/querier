import json
from datetime import datetime
from uuid import UUID


class Serializer(object):
    def __str__(self):
        return json.dumps(self, default=serialize, sort_keys=True, indent=4)

    def to_json(self):
        return json.dumps(self, default=serialize, sort_keys=True, indent=4)

    def __repr__(self):
        return json.dumps(self, default=serialize, sort_keys=True, indent=4)


def serialize(o):
    if type(o) is datetime:
        return o.isoformat()
    elif type(o) is UUID:
        return str(o)
    else:
        return o.__dict__


class ServiceOverview(Serializer):
    name: str

    def __init__(self, name, calls, p99, avgduration, error_count=0, timeframe=None) -> None:
        self.name = name
        self.calls = calls
        self.p99 = p99
        self.avgduration = avgduration
        self.error_count = error_count
        self.timeframe = timeframe
