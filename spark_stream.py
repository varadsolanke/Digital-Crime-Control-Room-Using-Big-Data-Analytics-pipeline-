from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, to_timestamp
from pyspark.sql.types import StringType, StructField, StructType

spark = SparkSession.builder.appName("CyberCrimeStream").getOrCreate()

schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("activity_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "cyber_logs")
    .load()
)

parsed_df = (
    df.selectExpr("CAST(value AS STRING) as value_json")
    .select(from_json(col("value_json"), schema).alias("log"))
    .select("log.*")
    .withColumn("event_ts", to_timestamp(col("timestamp")))
    .withColumn("event_date", to_date(col("event_ts")))
    .filter(
        col("user_id").isNotNull()
        & (col("user_id") != "")
        & (col("user_id") != "unknown")
        & col("status").isin("success", "failure")
    )
)

query = parsed_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
