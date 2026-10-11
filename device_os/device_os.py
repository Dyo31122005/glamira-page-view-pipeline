from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from user_agents import parse


def parse_os(ua):
    user_agent = parse(ua)
    return user_agent.os.family


parse_os_udf = udf(parse_os, returnType=StringType())
