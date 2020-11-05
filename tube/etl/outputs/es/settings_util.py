import yaml


def get_settings(config):
    settings_content = yaml.load(
        open(config.INDEX_SETTINGS_FILE), Loader=yaml.SafeLoader
    )
    return {"settings": settings_content}


def get_analyzer_settings(config):
    """
    Retrieves names and kinds of analyzers from settings.
    """
    analyzers = yaml.load(open(config.ANALYZERS_FILE), Loader=yaml.SafeLoader)
    return analyzers


def build_properties(config, field_types):
    """Translates Pythonic field types into ElasticSearch-friendly property dictionary.

    Specifically maps str types to the necessary text field with analyzer
    for searching partial matches against keyword field types.

    :param field_types: dictionary of field and their types
    :return: dictionary of field and their ES types and analyzer settings where applicable.

    E.g. given field_types = {"subject_id": [str], "file_size": [float]}, returns
        {'subject_id': {'type': 'keyword',
                        'fields': {
                            'analyzed': {
                                'type': 'text',
                                'analyzer': 'ngram_analyzer',
                                'search_analyzer': 'search_analyzer'
                            }
                        }},
        'file_size': {'type': 'float'}}
    """
    es_type = {str: "keyword", float: "float", int: "long"}
    analyzer_names_and_types = get_analyzer_settings(config)

    properties = {}
    str_analyzed_fields = {"type": "keyword", "fields": {"analyzed": {"type": "text"}}}

    for key in analyzer_names_and_types.keys():
        str_analyzed_fields["fields"]["analyzed"][key] = analyzer_names_and_types[key]

    for field_name, field_type_arr in field_types.items():
        field_type = field_type_arr[0]
        if field_type is not str:
            properties[field_name] = {"type": es_type[field_type]}
        else:
            properties[field_name] = str_analyzed_fields

    # explicitly map 'node_id'
    properties["node_id"] = {"type": "keyword"}

    return properties
