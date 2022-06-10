from tests.utils_es import get_item_from_elasticsearch
from tests.value.value import Value


class ESValue(Value):
    def __init__(self, parser, submitter_id, doc_type, names):
        super(ESValue, self).__init__(parser, submitter_id, doc_type, names)
        self.parser = parser
        self.submitter_id = submitter_id
        self.doc_type = doc_type
        self.names = names
        self.val, self.length = self.value()

    def __getattr__(self, item):
        if self.val is None:
            return None
        return self.val.__getattr__(item) if item in self.val else None

    def value(self):
        results = get_item_from_elasticsearch(
            self.parser.name, self.doc_type, self.submitter_id
        )
        result_length = len(results)
        if len(results) == 0:
            return None, 0
        result = results[0]
        return result, result_length
