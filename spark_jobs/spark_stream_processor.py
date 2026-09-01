from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, lit, when, round as spark_round, from_json
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
    .option("startingOffsets", "latest")       # ياخد الرسائل الجديدة بس (يحمي من الـ backlog)
    .option("maxOffsetsPerTrigger", 50)         # حد أقصى للرسائل في كل Batch
    .load()
)

applications_stream = (
    app_raw_stream
    .selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), cdc_schema).alias("data"))
    .select("data.payload.after.*", "data.payload.op")
    .filter(col("op") == "c")   # نهتم بالسجلات الجديدة (Insert) بس
)

# =========================================================
# 4. قراءة bureau كـ Static DataFrame من Parquet (مش Postgres)
# =========================================================
bureau_static_df = (
    spark.read.parquet("/mnt/d/RealTime-Credit-Banking-Infrastructure/data/lake/bureau.parquet")
    .select(
        col("sk_id_curr").cast("long").alias("bureau_sk_id_curr"),
        col("amt_credit_sum").cast("double").alias("amt_credit_sum"),
        col("amt_credit_sum_debt").cast("double").alias("amt_credit_sum_debt"),
        col("credit_active").cast("string").alias("credit_active")
    )
)

# =========================================================
# 5. الـ Stream-Static Join
# =========================================================
joined_stream = applications_stream.join(
    bureau_static_df,
    applications_stream.sk_id_curr == bureau_static_df.bureau_sk_id_curr,
    how="left_outer"
).drop("bureau_sk_id_curr")

# =========================================================
# 6. تنظيف الـ Nulls
# =========================================================
cleaned_stream = joined_stream \
    .withColumn("amt_income_total", coalesce(col("amt_income_total"), lit(1.0))) \
    .withColumn("amt_credit", coalesce(col("amt_credit"), lit(0.0))) \
    .withColumn("amt_credit_sum_debt", coalesce(col("amt_credit_sum_debt"), lit(0.0))) \
    .withColumn("amt_credit_sum", coalesce(col("amt_credit_sum"), lit(0.0))) \
    .withColumn("credit_active", coalesce(col("credit_active"), lit("Unknown")))

# =========================================================
# 7. حساب الـ Features وتقييم المخاطر
# =========================================================
features_stream = cleaned_stream \
    .withColumn("credit_to_income_ratio", spark_round(col("amt_credit") / col("amt_income_total"), 4)) \
    .withColumn("debt_to_income_ratio", spark_round(col("amt_credit_sum_debt") / col("amt_income_total"), 4))

final_stream = features_stream.withColumn(
    "is_high_risk",
    when(
        (col("credit_to_income_ratio") > 3.0) |
        (col("debt_to_income_ratio") > 0.5) |
        ((col("credit_active") == "Active") & (col("amt_credit_sum_debt") > col("amt_credit_sum") * 0.8)),
        1
    ).otherwise(0)
)

# =========================================================
# 8. الإخراج - Console للاختبار (لسه هنحوله Parquet بعدين)
# =========================================================
query = (
    final_stream.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()