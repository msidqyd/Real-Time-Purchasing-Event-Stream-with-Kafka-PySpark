# Real-Time Purchasing Stream Pipeline with Kafka & Spark

This project simulates a real-time purchasing transaction stream and processes it using Apache Kafka and Apache Spark Structured Streaming.

---

## Tech Stack

| Component        | Technology             |
|------------------|------------------------|
| Messaging        | Apache Kafka           |
| Stream Processing| Apache Spark (3.2+)    |
| Language         | Python (3.8+)          |
| Orchestration    | Docker + Docker Compose|
| Serialization    | JSON                   |

---

## Features

- Simulates purchase events with `goods`, `quantity`, `price`, `amount`, and `timestamp`
- Randomly injects **2% late events** (30–60 minutes late)
- Handles **late event processing** using Spark's `withWatermark()`
- Live stream output of transactions
- Aggregated 5-minute window summary per user
- Cleanly Dockerized for local development

---



## How It Works

- Clone this repo
- Make Docker Build
- Make Kafka
- Make Spark
- Make Run-Producer P=9
- Make Run-Consumer P=9

### Kafka Producer

Simulates continuous purchase events:
- `transaction_id`, `user_id`, `goods`, `quantity`, `price`, `amount`, `timestamp`
- 10% of events have timestamps 30–60 minutes earlier (simulating late events)


### Notes
- Requires Python 3.8+ and Spark 3.2+ (handled via Docker)
- Uses withWatermark("event_time", "1 hour") for late event handling
- Set startingOffsets to "latest" to avoid reprocessing old messages

### Show Late Event
```python
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

# Display late event

query1 = (
    json_df
    .filter((col("is_late") == 1) & (col("transaction_id").isNotNull()))
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

```
![Screenshot 2025-06-29 205646](https://github.com/user-attachments/assets/5bb0d26a-5b4b-432a-9a02-26b4def4660e)

### Show Aggregation
```python
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
# Display aggregation.
query2 = (
    transaction_agg.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)
```
![Screenshot 2025-06-29 205622](https://github.com/user-attachments/assets/7879bad9-5e73-448e-94bd-c8734301a6a4)

### Documentation
![Screenshot 2025-06-29 204216](https://github.com/user-attachments/assets/e34d4977-1efa-4726-ba90-0f93ffb782b1)
![image](https://github.com/user-attachments/assets/22efa0a8-07a2-4b51-97bf-6945c83f57a7)


