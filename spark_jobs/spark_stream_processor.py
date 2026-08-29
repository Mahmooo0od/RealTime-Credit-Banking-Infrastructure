import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. Spark Session متوافقة تماماً مع PySpark 3.5.0
spark = SparkSession.builder \
    .appName("CreditBanking-StreamEnrichment") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.caseSensitive", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Static Parquet Data Lake
PARQUET_PATH = "data/lake/bureau.parquet"
print(f"Loading Static Reference Data from {PARQUET_PATH}...")

raw_bureau = spark.read.parquet(PARQUET_PATH)

# توحيد أسماء الأعمدة إلى Lowercase
bureau_df = raw_bureau.toDF(*[c.lower() for c in raw_bureau.columns]) \
    .groupBy("sk_id_curr") \
    .agg(
        expr("count(sk_id_bureau)").alias("total_past_loans"),
        expr("sum(case when credit_active='Active' then 1 else 0 end)").alias("active_loans_count"),
        expr("coalesce(sum(amt_credit_sum_debt), 0)").alias("total_current_debt")
    )

# 3. Kafka Stream Input
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "banking.banking_core.applications"

streaming_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# 4. Debezium CDC Schema Parsing
payload_schema = StructType([
    StructField("after", StructType([
        StructField("sk_id_curr", IntegerType(), True),
        StructField("name_contract_type", StringType(), True),
        StructField("amt_income_total", DoubleType(), True),
        StructField("amt_credit", DoubleType(), True)
    ]), True)
])

schema = StructType([
    StructField("payload", payload_schema, True)
])

# 5. Extract JSON
json_df = streaming_raw_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.payload.after.*") \
    .filter(col("sk_id_curr").isNotNull())

# 6. Stream-Static Join
enriched_stream_df = parsed_df.join(
    bureau_df,
    on="sk_id_curr",
    how="left"
)

# 7. Output Console
query = enriched_stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("PySpark Streaming Engine Started successfully! Waiting for real-time applications...")
query.awaitTermination()
