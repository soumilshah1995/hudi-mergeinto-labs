![Screenshot 2024-07-31 at 8 30 19 AM](https://github.com/user-attachments/assets/0c8b28f0-cde0-4b48-9eef-0b4d0ebff28e)

docker-compose.yaml
```
version: "3"

services:


  metastore_db:
    image: postgres:11
    hostname: metastore_db
    ports:
      - 5432:5432
    environment:
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: hive
      POSTGRES_DB: metastore

  hive-metastore:
    hostname: hive-metastore
    image: 'starburstdata/hive:3.1.2-e.18'
    ports:
      - '9083:9083' # Metastore Thrift
    environment:
      HIVE_METASTORE_DRIVER: org.postgresql.Driver
      HIVE_METASTORE_JDBC_URL: jdbc:postgresql://metastore_db:5432/metastore
      HIVE_METASTORE_USER: hive
      HIVE_METASTORE_PASSWORD: hive
      HIVE_METASTORE_WAREHOUSE_DIR: s3://datalake/
      S3_ENDPOINT: http://minio:9000
      S3_ACCESS_KEY: admin
      S3_SECRET_KEY: password
      S3_PATH_STYLE_ACCESS: "true"
      REGION: ""
      GOOGLE_CLOUD_KEY_FILE_PATH: ""
      AZURE_ADL_CLIENT_ID: ""
      AZURE_ADL_CREDENTIAL: ""
      AZURE_ADL_REFRESH_URL: ""
      AZURE_ABFS_STORAGE_ACCOUNT: ""
      AZURE_ABFS_ACCESS_KEY: ""
      AZURE_WASB_STORAGE_ACCOUNT: ""
      AZURE_ABFS_OAUTH: ""
      AZURE_ABFS_OAUTH_TOKEN_PROVIDER: ""
      AZURE_ABFS_OAUTH_CLIENT_ID: ""
      AZURE_ABFS_OAUTH_SECRET: ""
      AZURE_ABFS_OAUTH_ENDPOINT: ""
      AZURE_WASB_ACCESS_KEY: ""
      HIVE_METASTORE_USERS_IN_ADMIN_ROLE: "admin"
    depends_on:
      - metastore_db
    healthcheck:
      test: bash -c "exec 6<> /dev/tcp/localhost/9083"

  minio:
    image: minio/minio
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
      - MINIO_DOMAIN=minio
    networks:
      default:
        aliases:
          - warehouse.minio
    ports:
      - 9001:9001
      - 9000:9000
    command: ["server", "/data", "--console-address", ":9001"]

  mc:
    depends_on:
      - minio
    image: minio/mc
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc config host add minio http://minio:9000 admin password) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/warehouse;
      /usr/bin/mc mb minio/warehouse;
      /usr/bin/mc policy set public minio/warehouse;
      tail -f /dev/null
      "



volumes:
  hive-metastore-postgresql:

networks:
  default:
    name: hudi
```

# start Spark sql 
```
base) soumilshah@Soumils-MBP ~ % spark-sql \
    --packages 'org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.2' \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
    --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
    --conf "spark.sql.catalogImplementation=hive" \
    --conf "spark.hadoop.hive.metastore.uris=thrift://localhost:9083" \
    --conf "spark.hadoop.fs.s3a.access.key=admin" \
    --conf "spark.hadoop.fs.s3a.secret.key=password" \
    --conf "spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9000" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "fs.s3a.signing-algorithm=S3SignerType"


```

# Commands
```
-- Create the customer target table as a Hudi table
CREATE TABLE customer_target (
                                 customer_id INT,
                                 name STRING,
                                 email STRING,
                                 signup_date TIMESTAMP
)
    USING HUDI
OPTIONS (
    type = 'COPY_ON_WRITE',
    primaryKey = 'customer_id',
    preCombineField = 'signup_date',
    path = 's3a://warehouse/customer_target',
    hoodie.datasource.write.operation = 'upsert'
);

-- Create or replace the temporary view for initial CDC data
CREATE OR REPLACE TEMP VIEW customer_cdc_source AS
SELECT
    1 AS customer_id,
    'Alice' AS name,
    'alice@example.com' AS email,
    TIMESTAMP '2023-01-01 10:00:00' AS signup_date,
    'I' AS op
UNION ALL
SELECT
    2 AS customer_id,
    'Bob' AS name,
    'bob@example.com' AS email,
    TIMESTAMP '2023-01-05 11:00:00' AS signup_date,
    'I' AS op
UNION ALL
SELECT
    3 AS customer_id,
    'Charlie' AS name,
    'charlie@example.com' AS email,
    TIMESTAMP '2023-01-10 12:00:00' AS signup_date,
    'I' AS op;

-- Perform the initial merge
MERGE INTO customer_target AS target
    USING customer_cdc_source AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED AND source.op = 'D' THEN
        DELETE
    WHEN MATCHED AND source.op IN ('I', 'U') THEN
        UPDATE SET
            target.name = source.name,
            target.email = source.email,
            target.signup_date = source.signup_date
    WHEN NOT MATCHED AND source.op = 'I' THEN
        INSERT (customer_id, name, email, signup_date)
            VALUES (source.customer_id, source.name, source.email, source.signup_date);

-- Verify the initial data
SELECT * FROM customer_target;

-- Create or replace the temporary view for updated CDC data
CREATE OR REPLACE TEMP VIEW customer_cdc_source AS
SELECT
    4 AS customer_id,
    'David' AS name,
    'david@example.com' AS email,
    TIMESTAMP '2023-02-01 13:00:00' AS signup_date,
    'I' AS op
UNION ALL
SELECT
    5 AS customer_id,
    'Eve' AS name,
    'eve@example.com' AS email,
    TIMESTAMP '2023-02-05 14:00:00' AS signup_date,
    'I' AS op
UNION ALL
SELECT
    2 AS customer_id,
    'Bob' AS name,
    'bob_updated@example.com' AS email,
    TIMESTAMP '2023-01-05 11:00:00' AS signup_date,
    'U' AS op
UNION ALL
SELECT
    3 AS customer_id,
    'Charlie' AS name,
    'charlie@example.com' AS email,
    TIMESTAMP '2023-01-10 12:00:00' AS signup_date,
    'D' AS op;

-- Perform the merge again with the updated CDC data
MERGE INTO customer_target AS target
    USING customer_cdc_source AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED AND source.op = 'D' THEN
        DELETE
    WHEN MATCHED AND source.op IN ('I', 'U') THEN
        UPDATE SET
            target.name = source.name,
            target.email = source.email,
            target.signup_date = source.signup_date
    WHEN NOT MATCHED AND source.op = 'I' THEN
        INSERT (customer_id, name, email, signup_date)
            VALUES (source.customer_id, source.name, source.email, source.signup_date);

-- Query the customer_target table to see the updated results
SELECT * FROM customer_target;



DROP table customer_target;

DROP TABLE customer_target;

```
