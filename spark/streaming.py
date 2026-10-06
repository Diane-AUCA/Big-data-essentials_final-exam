# Spark Structured Streaming: reads transactions from Kafka, predicts fraud
# with the trained model and saves the results to AWS RDS

import os
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

spark = (
    SparkSession.builder
    .appName("MoMo-Fraud-Streaming")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.driver.memory", "1500m")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# columns of each Kafka message (one CSV line)
SCHEMA = """
    transaction_id STRING, timestamp STRING, account_id STRING, provider STRING,
    customer_age INT, account_age_days INT, home_district STRING,
    transaction_type STRING, channel STRING, amount_rwf INT, balance_before INT,
    balance_after INT, counterparty_id STRING, tx_district STRING, tx_province STRING,
    is_home_district INT, is_new_device INT, sim_swap_7d INT, hour_of_day INT,
    day_of_week INT, is_weekend INT, tx_count_1h INT, avg_amount_7d INT,
    is_fraud INT, fraud_type STRING
"""

# load the model trained by train_model.py
model = PipelineModel.load("hdfs://namenode:8020/momo/models/fraud_model")
print("Model loaded from HDFS")

# read the Kafka topic as a stream
kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "momo-transactions")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 500)   # at most 500 messages per batch
    .load()
)

# turn each message (a CSV line) into columns
transactions = (
    kafka_stream
    .select(F.from_csv(F.col("value").cast("string"), SCHEMA).alias("t"))
    .select("t.*")
)

INSERT_SQL = """
INSERT IGNORE INTO predictions
(transaction_id, event_time, account_id, provider, transaction_type, channel,
 amount_rwf, balance_before, balance_after, counterparty_id, tx_district,
 tx_province, hour_of_day, fraud_probability, predicted_fraud, actual_fraud, fraud_type)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def save_batch(batch_df, batch_id):
    """Predict fraud for one micro-batch and save it to RDS."""
    if batch_df.isEmpty():
        return

    # the model adds the features, then the prediction and the probability
    scored = model.transform(batch_df)

    rows = scored.select(
        "transaction_id", "timestamp", "account_id", "provider",
        "transaction_type", "channel", "amount_rwf", "balance_before",
        "balance_after", "counterparty_id", "tx_district", "tx_province",
        "hour_of_day",
        F.round(vector_to_array("probability")[1], 4).alias("fraud_probability"),
        F.col("prediction").cast("int").alias("predicted_fraud"),
        "is_fraud", "fraud_type",
    ).collect()

    conn = pymysql.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ["DB_PORT"]),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
        ssl={"check_hostname": False},
    )
    cur = conn.cursor()
    new_rows = cur.executemany(INSERT_SQL, [tuple(r) for r in rows])  # duplicates are ignored
    conn.commit()
    conn.close()

    flagged = sum(r["predicted_fraud"] for r in rows)
    print(f"batch {batch_id}: {new_rows} new transactions saved to RDS, {flagged} flagged as fraud")


query = (
    transactions.writeStream
    .foreachBatch(save_batch)
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", "/tmp/momo_checkpoint")
    .start()
)

print("Streaming started - waiting for transactions from Kafka...")
query.awaitTermination()
