import hashlib
import os

import psycopg2
import pyspark.sql.functions as f
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField, LongType

import util.config as conf
from browser.browser import parse_browser_udf
from country.country import parse_country_udf
from device_os.device_os import parse_os_udf
from util.logger import Log4j

_FACT_TEMPLATE = """(
    %(id)s, %(date_key)s,
    (SELECT location_key FROM dim_location
      WHERE country_code = %(country_code)s
        AND COALESCE(region_name, '') = %(region_name)s
        AND COALESCE(city_name, '') = %(city_name)s),
    (SELECT product_key FROM dim_product WHERE product_id = %(product_id)s),
    (SELECT store_key FROM dim_store WHERE store_id = %(store_id)s),
    (SELECT customer_key FROM dim_customer WHERE device_id = %(device_id)s),
    %(browser)s, %(os)s, %(collection)s, %(current_url)s, %(referrer_url)s, %(local_time)s
)"""

if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))
    conf_path_file = base_dir + "/spark.conf"

    conf = conf.Config(conf_path_file)

    spark_conf = conf.spark_conf
    kafka_conf = conf.kafka_conf
    postgres_conf = conf.postgres_conf
    # Unqualified paths resolve against fs.defaultFS (HDFS on YARN, local filesystem
    # otherwise), so the writable location differs between local-mode testing and a
    # real YARN deploy - keep it overridable instead of hardcoding one or the other.
    checkpoint_location = os.environ.get("CHECKPOINT_LOCATION", "/user/spark/checkpoint/99-project")

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"kafka_conf: {kafka_conf}")
    log.info(f"postgres_conf: {postgres_conf}")

    value_schema = StructType([
        StructField("id", StringType()),
        StructField("time_stamp", LongType()),
        StructField("local_time", StringType()),
        StructField("ip", StringType()),
        StructField("user_agent", StringType()),
        StructField("resolution", StringType()),
        StructField("device_id", StringType()),
        StructField("api_version", StringType()),
        StructField("store_id", StringType()),
        StructField("current_url", StringType()),
        StructField("referrer_url", StringType()),
        StructField("email_address", StringType()),
        StructField("collection", StringType()),
        StructField("product_id", StringType()),
    ])

    raw_df = spark.readStream \
        .format("kafka") \
        .options(**kafka_conf) \
        .load()

    parsed_df = raw_df \
        .select(f.from_json(col("value").cast(StringType()), value_schema).alias("data")) \
        .select("data.*") \
        .withColumn("local_time", f.to_timestamp(col("local_time"), "yyyy-MM-dd HH:mm:ss"))


    def _connect():
        # Moi partition function tu goi ham nay de mo 1 connection RIENG cho ca
        # partition (khong phai moi row) - dung postgres_conf qua closure.
        return psycopg2.connect(
            host=postgres_conf["host"],
            port=postgres_conf["port"],
            dbname=postgres_conf["dbname"],
            user=postgres_conf["user"],
            password=postgres_conf["password"],
        )


    def upsert_partition_date(partition):
        date_rows = {}
        for row in partition:
            lt = row.local_time
            if lt is None:
                continue
            date_key = int(lt.strftime("%Y%m%d%H"))
            if date_key not in date_rows:
                date_rows[date_key] = (
                    date_key, lt.date(), lt.hour, lt.day, lt.month, lt.year,
                    (lt.month - 1) // 3 + 1, lt.strftime("%A"), lt.strftime("%B"),
                    lt.weekday() >= 5,
                )
        if not date_rows:
            return

        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO dim_date
                    (date_key, full_date, hour, day, month, year, quarter, day_name, month_name, is_weekend)
                    VALUES %s
                    ON CONFLICT (date_key) DO NOTHING
                """, list(date_rows.values()))
        finally:
            conn.close()


    def upsert_partition_location(partition):
        locations = {}
        for row in partition:
            loc = row.location
            if loc and loc.country_code:
                key = (loc.country_code, loc.region_name or "", loc.city_name or "")
                locations[key] = (loc.country_code, loc.country_name, loc.region_name, loc.city_name)
        if not locations:
            return

        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO dim_location (country_code, country_name, region_name, city_name)
                    VALUES %s
                    ON CONFLICT (country_code, (COALESCE(region_name, '')), (COALESCE(city_name, '')))
                    DO UPDATE SET country_name = EXCLUDED.country_name, updated_at = now()
                """, list(locations.values()))
        finally:
            conn.close()


    def upsert_partition_product(partition):
        product_ids = {row.product_id for row in partition if row.product_id}
        if not product_ids:
            return

        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO dim_product (product_id) VALUES %s
                    ON CONFLICT (product_id) DO UPDATE SET updated_at = now()
                """, [(p,) for p in product_ids])
        finally:
            conn.close()


    def upsert_partition_store(partition):
        store_ids = {row.store_id for row in partition if row.store_id}
        if not store_ids:
            return

        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO dim_store (store_id) VALUES %s
                    ON CONFLICT (store_id) DO UPDATE SET updated_at = now()
                """, [(s,) for s in store_ids])
        finally:
            conn.close()


    def upsert_partition_customer(partition):
        customers = {}
        for row in partition:
            if not row.device_id:
                continue
            email_hashed = hashlib.sha256(row.email_address.encode()).hexdigest() if row.email_address else None
            customers[row.device_id] = (email_hashed, bool(row.email_address))
        if not customers:
            return

        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO dim_customer (device_id, email_hashed, has_email)
                    VALUES %s
                    ON CONFLICT (device_id) DO UPDATE SET updated_at = now()
                """, [(d, v[0], v[1]) for d, v in customers.items()])
        finally:
            conn.close()


    def upsert_partition_fact(partition):
        # Dim table da duoc upsert xong o cac buoc truoc (xem process_batch) nen fact
        # chi can JOIN/subquery de resolve surrogate key ngay trong cau INSERT.
        fact_rows = []
        for row in partition:
            if row.local_time is None:
                continue
            loc = row.location
            fact_rows.append({
                "id": row.id,
                "date_key": int(row.local_time.strftime("%Y%m%d%H")),
                "country_code": loc.country_code if loc else None,
                "region_name": (loc.region_name or "") if loc else "",
                "city_name": (loc.city_name or "") if loc else "",
                "product_id": row.product_id,
                "store_id": row.store_id,
                "device_id": row.device_id,
                "browser": row.browser,
                "os": row.os,
                "collection": row.collection,
                "current_url": row.current_url,
                "referrer_url": row.referrer_url,
                "local_time": row.local_time,
            })
        if not fact_rows:
            return

        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO fact_page_view
                    (id, date_key, location_key, product_key, store_key, customer_key,
                     browser, os, collection, current_url, referrer_url, local_time)
                    VALUES %s
                    ON CONFLICT (id) DO NOTHING
                """, fact_rows, template=_FACT_TEMPLATE)
        finally:
            conn.close()


    def process_batch(batch_df, batch_id):
        enriched_df = batch_df \
            .withColumn("location", parse_country_udf(col("ip"))) \
            .withColumn("browser", parse_browser_udf(col("user_agent"))) \
            .withColumn("os", parse_os_udf(col("user_agent"))) \
            .persist()

        try:
            if enriched_df.isEmpty():
                return

            log.info(f"batch {batch_id}: upserting dims then fact via foreachPartition")

            # 1. Upsert moi dim truoc - moi partition tu mo 1 connection, bulk upsert.
            #    Day la cac Spark action, chay xong hoan toan (blocking) truoc khi toi dong tiep theo.
            enriched_df.foreachPartition(upsert_partition_date)
            enriched_df.foreachPartition(upsert_partition_location)
            enriched_df.foreachPartition(upsert_partition_product)
            enriched_df.foreachPartition(upsert_partition_store)
            enriched_df.foreachPartition(upsert_partition_customer)

            # 2. Dim da co du lieu - fact chi viec JOIN/subquery de resolve surrogate key
            enriched_df.foreachPartition(upsert_partition_fact)
        finally:
            enriched_df.unpersist()


    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", checkpoint_location) \
        .start()

    query.awaitTermination()
