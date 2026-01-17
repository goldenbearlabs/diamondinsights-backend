from abc import ABC, abstractmethod

class BaseAdapter(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def run(self, data):
        pass

    def _json_get(self, json, key, default=None):
        "Safe json get"
        if key not in json:
            return default
        return json[key]