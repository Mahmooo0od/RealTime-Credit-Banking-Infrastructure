from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, lit, when, round as spark_round, from_json,
    sum as spark_sum, count as spark_count
)
from pyspark.sql.types import (
    StructType, StringType, LongType, DoubleType
)

# =========================================================
# 1. إعداد الـ Spark Session
# =========================================================
spark = (
    SparkSession.builder
    .appName("RealTimeCreditBankingStream")
    .config("spark.driver.memory", "1500m")
    .config("spark.driver.maxResultSize", "512m")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================================================
# 2. Schema بيطابق شكل رسالة Debezium الحقيقي
#    (البيانات ملفوفة جوه "payload" -> "after")
# =========================================================
after_schema = StructType() \
    .add("sk_id_curr", LongType()) \
    .add("amt_income_total", DoubleType()) \
    .add("amt_credit", DoubleType()) \
    .add("amt_annuity", DoubleType())

payload_schema = StructType() \
    .add("after", after_schema) \
    .add("op", StringType())

cdc_schema = StructType() \
    .add("payload", payload_schema)

# =========================================================
# 3. قراءة الـ Real-time Stream من Kafka
# =========================================================
app_raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "banking.banking_core.applications")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 50)
    .load()
)

applications_stream = (
    app_raw_stream
    .selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), cdc_schema).alias("data"))
    .select("data.payload.after.*", "data.payload.op")
    .filter(col("op") == "c")
)

# =========================================================
# 4. قراءة bureau من Parquet + عمل الـ Aggregation
#    (صف واحد لكل عميل بدل التكرار) + Cache لتحسين الأداء
# =========================================================
bureau_raw_df = spark.read.parquet(
    "/mnt/d/RealTime-Credit-Banking-Infrastructure/data/lake/bureau.parquet"
)

bureau_aggregated_df = (
    bureau_raw_df
    .groupBy(col("sk_id_curr").alias("bureau_sk_id_curr"))
    .agg(
        spark_sum(col("amt_credit_sum").cast("double")).alias("total_credit_sum"),
        spark_sum(col("amt_credit_sum_debt").cast("double")).alias("total_credit_sum_debt"),
        spark_count(
            when(col("credit_active") == "Active", 1)
        ).alias("active_credits_count"),
        spark_count(lit(1)).alias("total_credits_count")
    )
    .cache()
)

# نجبر Spark يحسبها مرة واحدة فورًا بدل ما تتحسب من جديد مع كل Batch
_customer_count = bureau_aggregated_df.count()
print(f"Bureau aggregated and cached: {_customer_count} customers")

# =========================================================
# 5. الـ Stream-Static Join
# =========================================================
joined_stream = applications_stream.join(
    bureau_aggregated_df,
    applications_stream.sk_id_curr == bureau_aggregated_df.bureau_sk_id_curr,
    how="left_outer"
).drop("bureau_sk_id_curr")

# =========================================================
# 6. تنظيف الـ Nulls (لعملاء مالهمش سجل bureau أصلاً)
# =========================================================
cleaned_stream = joined_stream \
    .withColumn("amt_income_total", coalesce(col("amt_income_total"), lit(1.0))) \
    .withColumn("amt_credit", coalesce(col("amt_credit"), lit(0.0))) \
    .withColumn("total_credit_sum_debt", coalesce(col("total_credit_sum_debt"), lit(0.0))) \
    .withColumn("total_credit_sum", coalesce(col("total_credit_sum"), lit(0.0))) \
    .withColumn("active_credits_count", coalesce(col("active_credits_count"), lit(0))) \
    .withColumn("total_credits_count", coalesce(col("total_credits_count"), lit(0)))

# =========================================================
# 7. حساب الـ Features وتقييم المخاطر
# =========================================================
features_stream = cleaned_stream \
    .withColumn("credit_to_income_ratio", spark_round(col("amt_credit") / col("amt_income_total"), 4)) \
    .withColumn("debt_to_income_ratio", spark_round(col("total_credit_sum_debt") / col("amt_income_total"), 4))

final_stream = features_stream.withColumn(
    "is_high_risk",
    when(
        (col("credit_to_income_ratio") > 3.0) |
        (col("debt_to_income_ratio") > 0.5) |
        ((col("active_credits_count") > 0) & (col("total_credit_sum_debt") > col("total_credit_sum") * 0.8)),
        1
    ).otherwise(0)
)

# =========================================================
# 8. الإخراج - كتابة فعلية على Silver Layer (Parquet)
# =========================================================
output_path = "/mnt/d/RealTime-Credit-Banking-Infrastructure/data/silver/enriched_applications"
checkpoint_path = "/mnt/d/RealTime-Credit-Banking-Infrastructure/data/checkpoints/enriched_applications_v1"

query = (
    final_stream.writeStream
    .format("parquet")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()