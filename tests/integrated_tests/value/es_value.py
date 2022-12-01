from tests.integrated_tests.utils_es import get_item_from_elasticsearch
from tests.integrated_tests.value.value import Value


class ESValue(Value):
    def __init__(self, parser, submitter_id, doc_type, names):
        super(ESValue, self).__init__(parser, submitter_id, doc_type, names)
        self.parser = parser
        self.submitter_id = submitter_id
        self.doc_type = doc_type
        self.names = names
        self.val, self.length = self.value()

    def __getattr__(self, item):
        return self.val.__getattr__(item) if self.val and item in self.val else None

    def value(self):
        results = get_item_from_elasticsearch(
            self.parser.name, self.doc_type, self.submitter_id
        )
        result = results[0] if len(results) > 0 else None
        return result, len(results)
