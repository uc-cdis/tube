from tests.value.value import Value


class CollectorValue(Value):
    def __init__(self, parser, submitter_id, doc_type, names):
        super(CollectorValue, self).__init__(parser, submitter_id, doc_type, names)
        self.parser = parser
        self.submitter_id = submitter_id
        self.doc_type = doc_type
        self.names = names
        self.val = None

    def __getattr__(self, item):
        return None
