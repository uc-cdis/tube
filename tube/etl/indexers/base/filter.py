class Filter():
    def __init__(self, json_filter):
        self.op = json_filter.get("op")
        self.props = json_filter.get("props")
