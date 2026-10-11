import os

import IP2Location
from pyspark import SparkFiles
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructField, StructType

# DB1 LITE chi co country. Khi nang cap len DB5 LITE (co them region + city),
# chi can thay file nay bang file DB5 (giu nguyen ten _DB_FILENAME ben duoi,
# hoac doi ten cho khop va cap nhat --files trong README) - code khong doi.
_DB_FILENAME = "IP2LOCATION-LITE-DB1.BIN"
_UNAVAILABLE_PREFIX = "This parameter is unavailable"
_db = None

country_schema = StructType([
    StructField("country_code", StringType()),
    StructField("country_name", StringType()),
    StructField("region_name", StringType()),
    StructField("city_name", StringType()),
])


def _resolve_db_path():
    # SparkFiles.get() only resolves once the file has been shipped via spark-submit
    # --files (needed on YARN, where UDFs run on remote executors); fall back to the
    # local data/ folder for local-mode runs where the project dir is already on disk.
    spark_files_path = SparkFiles.get(_DB_FILENAME)
    if os.path.exists(spark_files_path):
        return spark_files_path

    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", _DB_FILENAME)


def _get_db():
    global _db
    if _db is None:
        _db = IP2Location.IP2Location(_resolve_db_path())
    return _db


def _clean(value):
    # DB1 doesn't carry region/city - IP2Location returns a human-readable
    # "upgrade your data file" message instead of empty string for those fields.
    if value and not value.startswith(_UNAVAILABLE_PREFIX):
        return value
    return None


def parse_country(ip):
    rec = _get_db().get_all(ip)
    return rec.country_short, rec.country_long, _clean(rec.region), _clean(rec.city)


parse_country_udf = udf(parse_country, returnType=country_schema)
