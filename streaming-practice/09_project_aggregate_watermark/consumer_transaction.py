from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum, when, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
import os
# Get Kafka host from env configuration.
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
# Initialize spark session.
spark = (
    SparkSession.builder.appName("PurchasingAggregation")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
#Define schema to consumer
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("goods", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", LongType(), True),
])
#Read stream for purchasing event.
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:9092")  
    .option("subscribe", "purchasing")  
    .option("startingOffsets", "latest")
    .load()
)

# Show late data only with more than 30 minutes before ingestion time. 
json_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .filter(col("data").isNotNull())  
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
    .withColumn("transaction_time", col("timestamp").cast("timestamp"))
    .withColumn("window_1h", window(col("timestamp").cast("timestamp"), "1 hour"))
    .withColumn("ingestion_time", current_timestamp())
    .withColumn(
        "is_late",
        when(col("event_time") < col("ingestion_time") - expr("INTERVAL 30 MINUTES"), 1).otherwise(0)
    )
)




#Transform data before do aggregation.
json_df2 = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
    .withColumn("ingestion_time", current_timestamp())
    .withColumn(
        "is_late",
        when(col("event_time") < col("ingestion_time") - expr("INTERVAL 30 MINUTES"), 1).otherwise(0)
    )
)

# Stateful aggregation: sum amount and count transaction group by window.
# Do aggregation every 5 minutes with capture 1 hour for late data.
transaction_agg = (
    json_df2
    .withWatermark("event_time", "1 hour")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    )
    .agg(
        count("*").alias("transaction_count"),
        sum("is_late").alias("late_transaction"),
        sum("amount").alias("total_amount")
    )
)
#Display late event
query1 = (
    json_df
    .filter((col("is_late") == 1) & (col("transaction_id").isNotNull()))
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)


# Display aggregation.
query2 = (
    transaction_agg.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)

query1.awaitTermination()
query2.awaitTermination()
