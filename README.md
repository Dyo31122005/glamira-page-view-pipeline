# Glamira Page View Pipeline

Pipeline xử lý log hành vi người dùng (clickstream) thời gian thực: đọc từ Kafka
bằng Spark Structured Streaming, làm giàu dữ liệu (quốc gia từ IP, browser/OS từ
user-agent), lưu vào Postgres theo mô hình star schema, và hiển thị báo cáo qua
dashboard Streamlit.

```
Kafka (topic "product_view")
   │  log JSON: id, ip, user_agent, product_id, store_id, device_id, ...
   ▼
Spark Structured Streaming (page_view_streaming.py)
   │  - parse JSON
   │  - enrich: ip -> country (IP2Location), user_agent -> browser/os
   │  - foreachBatch -> foreachPartition: upsert dim truoc, fact sau
   ▼
Postgres (star schema: fact_page_view + 5 dim)
   ▼
Streamlit dashboard (app/app.py)
```

## Thiết kế database

Star schema — 1 bảng fact + 5 bảng dimension. Xem [sql/schema.sql](sql/schema.sql)
hoặc paste [sql/schema.dbml](sql/schema.dbml) vào [dbdiagram.io](https://dbdiagram.io/d)
để xem sơ đồ trực quan.

- `fact_page_view` — 1 dòng / 1 lượt xem trang, chỉ giữ surrogate key + thuộc tính sự kiện
- `dim_date` — granularity theo giờ (`date_key` dạng `yyyyMMddHH`)
- `dim_location` — quốc gia từ IP (region/city để sẵn cột, NULL cho đến khi dùng
  `IP2Location DB5 LITE` trở lên — xem [data/README.md](data/README.md))
- `dim_product`, `dim_store`, `dim_customer` — dedup theo natural key, `dim_customer`
  hash email bằng SHA-256, không lưu email thô

## Báo cáo ([sql/reports.sql](sql/reports.sql))

1. Top 10 `product_id` view nhiều nhất hôm nay
2. Top 10 quốc gia view nhiều nhất hôm nay
3. Top 5 `referrer_url` view nhiều nhất hôm nay
4. Danh sách `store_id` + lượt view theo 1 quốc gia, giảm dần
5. View phân bố theo giờ của 1 `product_id`
6. View theo giờ của từng `browser`, `os`

## Yêu cầu trước khi chạy

- Docker + Docker Compose
- 1 Kafka broker có topic chứa log dạng JSON tương tự `sql/schema.sql`'s `fact_page_view`
  (các field: `id`, `ip`, `user_agent`, `local_time`, `device_id`, `email_address`,
  `store_id`, `product_id`, `collection`, `current_url`, `referrer_url`)
- File `IP2LOCATION-LITE-DB1.BIN` (hoặc cao hơn) — xem [data/README.md](data/README.md)
  để tải miễn phí

## Cách chạy

### 1. Cấu hình

```shell
cp .env.example .env
# sua .env: dien KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_JAAS_CONFIG, POSTGRES_PASSWORD that
```

### 2. Dựng hạ tầng (Hadoop YARN + Postgres)

```shell
docker network create streaming-network --driver bridge   # neu chua co

cd infra/hadoop && docker compose up -d && cd ../..
cd infra/postgres && set -a && source ../../.env && set +a && docker compose up -d && cd ../..
```

### 3. Tạo schema Postgres

```shell
set -a && source .env && set +a
docker run --rm --network streaming-network -e PGPASSWORD="$POSTGRES_PASSWORD" \
  -v ./sql:/sql \
  postgres:16.3 psql -h postgres -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /sql/schema.sql
```

### 4. Chạy pipeline streaming

**Test nhanh (local mode):**

```shell
set -a && source .env && set +a
docker run --rm -ti --name page-view-pipeline \
--network=streaming-network \
-v ./:/app \
-e CHECKPOINT_LOCATION=/tmp/checkpoint \
-e KAFKA_BOOTSTRAP_SERVERS -e KAFKA_SASL_JAAS_CONFIG -e KAFKA_SUBSCRIBE \
-e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 -e POSTGRES_USER -e POSTGRES_PASSWORD -e POSTGRES_DB \
unigap/spark:3.5 bash -c "
pip install -q user-agents ua-parser IP2Location psycopg2-binary &&
cd /app &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
--master 'local[2]' \
page_view_streaming.py"
```

**Deploy YARN thật** (cần cụm YARN từ bước 2, conda-pack để đóng gói thư viện cho executor):

```shell
set -a && source .env && set +a
docker run --rm -ti --name page-view-pipeline \
--network=streaming-network \
-v ./:/app \
-v spark_lib:/home/spark/.ivy2 \
-e HADOOP_CONF_DIR=/app/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' -e PYSPARK_PYTHON='./environment/bin/python' \
-e KAFKA_BOOTSTRAP_SERVERS -e KAFKA_SASL_JAAS_CONFIG -e KAFKA_SUBSCRIBE \
-e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 -e POSTGRES_USER -e POSTGRES_PASSWORD -e POSTGRES_DB \
unigap/spark:3.5 bash -c "
(cd /app && zip -r /tmp/libs.zip browser/* country/* device_os/* util/*) &&
conda env create --file /app/environment.yml &&
source ~/miniconda3/bin/activate && conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/libs.zip \
--files /app/data/IP2LOCATION-LITE-DB1.BIN \
--deploy-mode client --master yarn \
/app/page_view_streaming.py"
```

Lưu ý: `conda pack` phải chạy ở working dir mặc định của container (không `cd`
vào thư mục mount volume trước đó) — khác filesystem nên `os.rename` nội bộ của
`conda-pack` sẽ lỗi `Invalid cross-device link`/`Permission denied`.

`CHECKPOINT_LOCATION`/path không scheme sẽ tự resolve theo `fs.defaultFS`
(HDFS khi có `HADOOP_CONF_DIR`, local filesystem khi không) — mặc định
`/user/spark/checkpoint/...` chỉ ghi được trên HDFS, cần override khi test local.

### 5. Chạy dashboard

```shell
cd app && pip install -r requirements.txt
POSTGRES_HOST=localhost POSTGRES_PORT=5433 POSTGRES_PASSWORD=*** streamlit run app.py
```

## Kiến trúc xử lý batch

`page_view_streaming.py` dùng `foreachBatch` → `foreachPartition`: mỗi partition
tự mở 1 connection Postgres, bulk-upsert (không mở connection theo từng dòng).
Dim được upsert xong hoàn toàn (action blocking) trước khi fact chạy; fact
resolve surrogate key bằng subquery ngay trong câu `INSERT` (không cần map
Python chia sẻ giữa các executor, vì mỗi partition chạy trên 1 process riêng).

## Bảo mật

Repo này **không chứa** credential thật nào — mọi secret (Kafka SASL password,
Postgres password) đọc qua biến môi trường (`.env`, không commit). File
`IP2LOCATION-LITE-DB1.BIN` cũng không được commit (license không cho phép
redistribute) — tự tải theo hướng dẫn ở [data/README.md](data/README.md).
