class Value(object):
    def __init__(self, parser, submitter_id, doc_type, names):
        self.parser = parser
        self.submitter_id = submitter_id
        self.doc_type = doc_type
        self.names = names
        self.val = None

    def __getattr__(self, item):
        pass

    def __eq__(self, other):
        for name in self.names:
            if self.__getattr__(name) != other.__getattr__(name):
                return False
        return True


def value_diff(left, right):
    equal = True
    if (
        left.names != right.names
        or (left.val is None and right.val is not None)
        or (left.val is not None and right.val is None)
    ):
        equal = False

    diffs = ["attr: left != right"]
    for name in left.names:
        print(left)
        print(left.__dict__)
        left_val = left.val.get(name)
        right_val = right.__getattr__(name)

        if isinstance(right_val, list) and left_val is not None:
            # This should check that left_val is an AttrList, probably
            items_not_equal = sorted(left_val) != sorted(right_val)
        elif isinstance(right_val, float):
            items_not_equal = abs(left_val - right_val) > 0.001
        else:
            items_not_equal = left_val != right_val

        if items_not_equal and left_val is not None and right_val is not None:
            equal = False
            diff = "{attr}: {left_val} != {right_val}".format(
                attr=name, left_val=left_val, right_val=right_val
            )
            types = "types: {left_type} != {right_type}".format(
                left_type=type(left_val), right_type=type(right_val)
            )
            diffs.append(diff)
            print(diff)
            diffs.append(types)
            print(types)

    return equal, diffs
