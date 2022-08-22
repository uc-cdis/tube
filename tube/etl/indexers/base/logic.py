from pyspark.sql.functions import col, array_contains, expr


def create_filter_from_json(json_filter):
    if json_filter is None:
        return None
    if json_filter.get("op").lower() in ["and", "or"]:
        return CompoundLogic(json_filter)
    else:
        return SimpleLogic(json_filter)


def build_filter_query(current_logic, new_fields, fields_to_be_removed):
    if type(current_logic) == CompoundLogic:
        return f" {current_logic.op} ".join(
            [
                build_filter_query(logic, new_fields, fields_to_be_removed)
                for logic in current_logic.logics
            ]
        )
    if type(current_logic) == SimpleLogic:
        if current_logic.op == "in" and type(current_logic.value) == list:
            s_value = ",".join(s for s in current_logic.value)
            value_in_query = f"({s_value})"
        elif type(current_logic.value) == str:
            value_in_query = f"'{current_logic.value}'"
        else:
            value_in_query = current_logic.value
        if current_logic.op == "contains":
            prop_name = current_logic.prop.replace(".", "__")
            new_fields.append(expr(f"array_contains({prop_name}, {value_in_query})"))
            new_field_name = f"__new_field_condition_{len(new_fields)}"
            fields_to_be_removed.extend([prop_name, new_field_name])
            return f"{new_field_name} = True"
        return f"{current_logic.prop} {current_logic.op} {value_in_query}"


def execute_filter_query(df_to_be_filtered, new_fields, conditions):
    i = 0
    while i < len(new_fields):
        df_to_be_filtered = df_to_be_filtered.withColumn(
            f"__new_field_condition_{i + 1}", new_fields[i]
        )
        i += 1
    return df_to_be_filtered.filter(conditions)


def execute_filter(df_to_be_filter, filter):
    new_fields = []
    fields_to_be_removed = []
    conditions = build_filter_query(filter, new_fields, fields_to_be_removed)
    filtered_df = execute_filter_query(df_to_be_filter, new_fields, conditions)
    filtered_df = filtered_df.drop(*fields_to_be_removed)
    return filtered_df


class BaseLogic:
    def __init__(self, json_filter):
        self.op = json_filter.get("op")

    def get_props(self) -> list:
        pass

    def get_agg_props(self) -> list:
        pass


class SimpleLogic(BaseLogic):
    def __init__(self, json_filter):
        super().__init__(json_filter)
        self.prop = json_filter.get("prop")
        self.value = json_filter.get("value")

    def get_props(self) -> list:
        return [self.prop]

    def get_agg_props(self) -> list:
        if self.op == "contains":
            return [self.prop]
        return []


class CompoundLogic(BaseLogic):
    def __init__(self, json_filter):
        # op of a ComplexLogic can be only either AND or OR
        super().__init__(json_filter)
        self.logics = [
            SimpleLogic(logic)
            if logic.get("op").lower() not in ["and", "or"]
            else CompoundLogic(logic)
            for logic in json_filter.get("logics")
        ]
        self.all_props = self._collect_all_props()

    def get_props(self) -> list:
        return self.all_props

    def get_agg_props(self) -> list:
        agg_props = []
        for logic in self.logics:
            agg_props.extend(logic.get_agg_props())
        return agg_props

    def _collect_all_props(self):
        all_props = []
        for logic in self.logics:
            all_props.extend(logic.get_props())
        return all_props
