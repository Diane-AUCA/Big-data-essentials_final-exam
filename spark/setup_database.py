# Connect to AWS RDS, create the tables and save the model results

import json
import os
import pymysql

conn = pymysql.connect(
    host=os.environ["DB_HOST"],
    port=int(os.environ["DB_PORT"]),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    database=os.environ["DB_NAME"],
    ssl={"check_hostname": False},  # encrypted connection
)
cur = conn.cursor()

cur.execute("SELECT VERSION()")
print("Connected to RDS, MySQL version:", cur.fetchone()[0])

# table for the streaming results
cur.execute("""
CREATE TABLE IF NOT EXISTS predictions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    event_time DATETIME,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    account_id VARCHAR(12),
    provider VARCHAR(10),
    transaction_type VARCHAR(20),
    channel VARCHAR(10),
    amount_rwf INT,
    balance_before INT,
    balance_after INT,
    counterparty_id VARCHAR(20),
    tx_district VARCHAR(20),
    tx_province VARCHAR(20),
    hour_of_day INT,
    fraud_probability DOUBLE,
    predicted_fraud INT,
    actual_fraud INT,
    fraud_type VARCHAR(30),
    INDEX (processed_at)
)
""")

# tables for the training results
cur.execute("""
CREATE TABLE IF NOT EXISTS model_metrics (
    model_name VARCHAR(40) PRIMARY KEY,
    auc_roc DOUBLE,
    auc_pr DOUBLE,
    precision_score DOUBLE,
    recall_score DOUBLE,
    f1_score DOUBLE,
    accuracy DOUBLE,
    is_best INT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS feature_importance (
    feature VARCHAR(40) PRIMARY KEY,
    importance DOUBLE
)
""")
print("Tables created")

# copy the results from model_metrics.json into the database
with open("/app/spark/model_metrics.json") as f:
    metrics = json.load(f)

for r in metrics["results"]:
    is_best = 1 if r["model"] == metrics["best_model"] else 0
    cur.execute(
        "REPLACE INTO model_metrics VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (r["model"], r["auc_roc"], r["auc_pr"], r["precision"], r["recall"],
         r["f1"], r["accuracy"], is_best),
    )

cur.execute("DELETE FROM feature_importance")
for item in metrics["top_features"]:
    cur.execute("INSERT INTO feature_importance VALUES (%s, %s)",
                (item["feature"], item["importance"]))

conn.commit()
print("Model results saved")

# show what is in the database now
cur.execute("SHOW TABLES")
for (table,) in cur.fetchall():
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    print(table, "->", cur.fetchone()[0], "rows")

conn.close()
