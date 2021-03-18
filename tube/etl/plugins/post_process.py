import pyspark.sql.functions as f
from pyspark.sql.types import *

from tube.settings import PROJECT_TO_RESOURCE_PATH


def create_single_auth_resource_path(project_id):
    s = project_id.split("-", 1)
    if len(s) < 2:
        return ""
    resource_path = PROJECT_TO_RESOURCE_PATH.get(s[1])
    if resource_path is None:
        return "/programs/{}/projects/{}".format(s[0], s[1])
    return resource_path


def project_id_to_auth_resource_path(project_id):
    if project_id is not None:
        if isinstance(project_id, list):
            return [create_single_auth_resource_path(p) for p in project_id]
        else:
            return create_single_auth_resource_path(project_id)
    else:
        return ""


def add_auth_resource_path(datarow):
    # add 'auth_resource_path' to resulting es document if 'project_id' exist
    if "project_id" in datarow[1]:
        datarow[1]["auth_resource_path"] = project_id_to_auth_resource_path(
            datarow[1]["project_id"]
        )

    return datarow[0], datarow[1]


udf_auth = f.udf(project_id_to_auth_resource_path, StringType())


def add_auth_resource_path_to_dataframe(df):
    df = df.withColumn("auth_resource_path", udf_auth("project_id"))
    return df
