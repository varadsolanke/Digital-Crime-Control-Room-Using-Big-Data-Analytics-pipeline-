from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, to_timestamp
from pyspark.sql.types import StringType, StructField, StructType

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "cyber_logs"
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/cyber_logs"
CHECKPOINT_PATH = "hdfs://namenode:9000/cyber_logs_checkpoint"

spark = (
    SparkSession.builder.appName("DigitalCrimeControlRoomStream")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("activity_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

json_df = raw_df.selectExpr("CAST(value AS STRING) AS value_json")
parsed_df = json_df.select(from_json(col("value_json"), schema).alias("json_data")).select("json_data.*")

clean_df = (
    parsed_df.withColumn("event_ts", to_timestamp(col("timestamp")))
    .withColumn("event_date", to_date(col("event_ts")))
    .filter(
        col("user_id").isNotNull()
        & (col("user_id") != "")
        & (col("user_id") != "unknown")
        & col("activity_type").isNotNull()
        & col("event_ts").isNotNull()
        & col("ip_address").isNotNull()
        & col("status").isin("success", "failure")
    )
)

query = (
    clean_df.writeStream.outputMode("append")
    .format("parquet")
    .option("path", HDFS_OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("event_date")
    .start()
)

query.awaitTermination()
