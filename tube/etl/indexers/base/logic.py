class BaseLogic:
    def __init__(self, json_filter):
        self.op = json_filter.get("op")


class SimpleLogic(BaseLogic):
    def __init__(self, json_filter):
        super().__init__(json_filter)
        self.prop = json_filter.get("prop")
        self.value = json_filter.get("value")


class CompoundLogic(BaseLogic):
    def __init__(self, json_filter):
        # op of a ComplexLogic can be only either AND or OR
        super().__init__(json_filter)
        self.logics = [
            SimpleLogic(logic) if logic.get("op").lower() not in ["and", "or"] else CompoundLogic(logic)
            for logic in json_filter.get("logics")
        ]
        self.children_node_logics = []  # list of compound logic to all the children (like summary_drug_use)
        self.all_props = self.get_all_props()

    def get_all_props(self):
        all_props = []
        for l in self.logics:
            if type(l) == SimpleLogic:
                all_props.append(l.prop)
            elif type(l) == CompoundLogic:
                all_props.extend(l.all_props)
        return all_props

# ((year_of_birth > 16 and gender = "male") or (year_of_birth > 18 and gender = "Female") and (vital_status = "S"))
# - op: and
#   logics:
#   - op: or
#     logics:
#      - op: and
#        logics:
#        - prop: year_of_brith
#          op: >
#          value: 16
#        - prop: gender
#          op: =
#          value: Male
#      - op: and
#        logics:
#          - prop: year_of_brith
#            op: >
#            value: 18
#          - prop: gender
#            op: =
#            value: Female
#
