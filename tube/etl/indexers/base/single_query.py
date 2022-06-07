from tube.etl.indexers.base.logic import CompoundLogic, SimpleLogic


def create_filter_from_json(json_filter):
    if json_filter is None:
        return None
    if json_filter.get("op").lower() in ["and", "or"]:
        return CompoundLogic(json_filter)
    else:
        return SimpleLogic(json_filter)


class SingleQuery:
    def __init__(self, props, filter_json):
        self.props = props
        self.filter = create_filter_from_json(filter_json)

    def get_filter_props(self):
        return self.filter.all_props

    # filter: follow_up.id == 'a' and summary_drug_use.id == 'b'

    # --> filter1: follow_up = 'a', related_filter:
    # --> filter2: summary_drug_use.id = 'b'


    # SELECT aliquot_status as status2, name, id FROM aliquots WHERE state_name == name1
    # SELECT aliquot_status as status1, name, id FROM aliquots WHERE state_name == name2
