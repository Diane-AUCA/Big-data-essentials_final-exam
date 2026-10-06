"""
Mobile Money Fraud Detection - Rwanda
Step: Train the fraud detection model with Spark MLlib

What this script does:
  1. Reads the historical transactions from HDFS
  2. Splits them by time: older data for training, newest months for testing
  3. Balances the training data (fraud is only ~1% of transactions)
  4. Builds an ML pipeline: feature engineering -> encoding -> model
  5. Trains Logistic Regression and Random Forest, and compares them
  6. Saves the best model to HDFS (the streaming job will load it later)

Run inside the spark container:
  docker exec spark python /app/spark/train_model.py
"""

import json
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, SQLTransformer, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# ---------------------------------------------------------------
# 1. Settings
# ---------------------------------------------------------------
HDFS_DATA = "hdfs://namenode:8020/momo/raw/mobile_money_transactions.csv"
HDFS_MODEL = "hdfs://namenode:8020/momo/models/fraud_model"
METRICS_FILE = "/app/spark/model_metrics.json"   # saved in your project folder

TEST_START_DATE = "2026-01-01"   # train on 2024-2025, test on Jan-May 2026
LEGIT_SAMPLE_RATE = 0.10         # keep 10% of normal transactions for training
SEED = 42

# The CSV columns and their types (faster and safer than letting Spark guess)
SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("account_id", StringType()),
    StructField("provider", StringType()),
    StructField("customer_age", IntegerType()),
    StructField("account_age_days", IntegerType()),
    StructField("home_district", StringType()),
    StructField("transaction_type", StringType()),
    StructField("channel", StringType()),
    StructField("amount_rwf", IntegerType()),
    StructField("balance_before", IntegerType()),
    StructField("balance_after", IntegerType()),
    StructField("counterparty_id", StringType()),
    StructField("tx_district", StringType()),
    StructField("tx_province", StringType()),
    StructField("is_home_district", IntegerType()),
    StructField("is_new_device", IntegerType()),
    StructField("sim_swap_7d", IntegerType()),
    StructField("hour_of_day", IntegerType()),
    StructField("day_of_week", IntegerType()),
    StructField("is_weekend", IntegerType()),
    StructField("tx_count_1h", IntegerType()),
    StructField("avg_amount_7d", IntegerType()),
    StructField("is_fraud", IntegerType()),
    StructField("fraud_type", StringType()),
])

# ---------------------------------------------------------------
# 2. Feature engineering (written in SQL so it is easy to read).
#    It is part of the model pipeline, so the streaming job gets
#    exactly the same features automatically.
# ---------------------------------------------------------------
FEATURE_SQL = """
SELECT *,
    amount_rwf / (balance_before + 1)                              AS amount_to_balance,
    CASE WHEN balance_after < 0.1 * balance_before THEN 1 ELSE 0 END AS account_emptied,
    amount_rwf / (avg_amount_7d + 1)                               AS amount_vs_7d_avg,
    CASE WHEN hour_of_day >= 23 OR hour_of_day <= 4 THEN 1 ELSE 0 END AS is_night,
    CASE WHEN transaction_type IN ('CASH_OUT', 'P2P_TRANSFER', 'BANK_TRANSFER')
         THEN 1 ELSE 0 END                                         AS is_money_out,
    sim_swap_7d * is_new_device                                    AS sim_swap_new_device,
    CASE WHEN account_age_days < 60 THEN 1 ELSE 0 END              AS is_new_account
FROM __THIS__
"""

CATEGORICAL_COLUMNS = ["transaction_type", "channel", "provider", "tx_province"]

NUMERIC_COLUMNS = [
    # original columns
    "amount_rwf", "balance_before", "customer_age", "account_age_days",
    "is_home_district", "is_new_device", "sim_swap_7d", "hour_of_day",
    "is_weekend", "tx_count_1h", "avg_amount_7d",
    # engineered columns (from FEATURE_SQL)
    "amount_to_balance", "account_emptied", "amount_vs_7d_avg", "is_night",
    "is_money_out", "sim_swap_new_device", "is_new_account",
]


def build_pipeline(classifier):
    """Feature engineering + text-to-number encoding + the model."""
    feature_step = SQLTransformer(statement=FEATURE_SQL)

    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep")
        for c in CATEGORICAL_COLUMNS
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{c}_index" for c in CATEGORICAL_COLUMNS],
        outputCols=[f"{c}_vec" for c in CATEGORICAL_COLUMNS],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=NUMERIC_COLUMNS + [f"{c}_vec" for c in CATEGORICAL_COLUMNS],
        outputCol="features",
    )
    return Pipeline(stages=[feature_step, *indexers, encoder, assembler, classifier])


def evaluate(name, predictions):
    """Compute the metrics that matter for fraud (accuracy alone is misleading)."""
    auc_roc = BinaryClassificationEvaluator(labelCol="is_fraud", metricName="areaUnderROC").evaluate(predictions)
    auc_pr = BinaryClassificationEvaluator(labelCol="is_fraud", metricName="areaUnderPR").evaluate(predictions)

    counts = predictions.groupBy("is_fraud", "prediction").count().collect()
    cm = {(int(r["is_fraud"]), int(r["prediction"])): r["count"] for r in counts}
    tp, fp = cm.get((1, 1), 0), cm.get((0, 1), 0)
    fn, tn = cm.get((1, 0), 0), cm.get((0, 0), 0)

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    accuracy = (tp + tn) / (tp + tn + fp + fn)

    print(f"\n----- {name} -----")
    print(f"AUC-ROC   : {auc_roc:.4f}")
    print(f"AUC-PR    : {auc_pr:.4f}")
    print(f"Precision : {precision:.4f}  (of transactions flagged as fraud, how many really were)")
    print(f"Recall    : {recall:.4f}  (of all real frauds, how many we caught)")
    print(f"F1-score  : {f1:.4f}")
    print(f"Accuracy  : {accuracy:.4f}")
    print("Confusion matrix:")
    print(f"                 predicted normal   predicted fraud")
    print(f"  real normal    {tn:>16,}   {fp:>15,}")
    print(f"  real fraud     {fn:>16,}   {tp:>15,}")

    return {
        "model": name, "auc_roc": round(auc_roc, 4), "auc_pr": round(auc_pr, 4),
        "precision": round(precision, 4), "recall": round(recall, 4),
        "f1": round(f1, 4), "accuracy": round(accuracy, 4),
        "confusion_matrix": {"tn": tn, "fp": fp, "fn": fn, "tp": tp},
    }


def main():
    start = time.time()
    spark = (
        SparkSession.builder
        .appName("MoMo-Fraud-Training")
        .config("spark.driver.memory", "1800m")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ---- Read data from HDFS ----
    print("Reading data from HDFS...")
    df = spark.read.csv(HDFS_DATA, header=True, schema=SCHEMA)
    # fraud_type tells the answer directly, so it must NOT be used for training
    df = df.drop("fraud_type")

    # ---- Split by time ----
    train_full = df.filter(F.col("timestamp") < TEST_START_DATE)
    # keep the test set in memory (spills to disk if needed) so it is read from HDFS only once
    test = df.filter(F.col("timestamp") >= TEST_START_DATE).persist(StorageLevel.MEMORY_AND_DISK)

    # ---- Balance the training data: all fraud + a sample of normal ----
    fraud = train_full.filter("is_fraud = 1")
    legit = train_full.filter("is_fraud = 0").sample(fraction=LEGIT_SAMPLE_RATE, seed=SEED)
    train = fraud.union(legit).cache()

    n_train_fraud = fraud.count()
    n_train = train.count()
    n_test = test.count()
    print(f"Training rows : {n_train:,} ({n_train_fraud:,} fraud) - data before {TEST_START_DATE}")
    print(f"Test rows     : {n_test:,} - data from {TEST_START_DATE} (never seen in training)")

    # ---- Train and compare two models ----
    models = {
        "Logistic Regression": LogisticRegression(
            labelCol="is_fraud", featuresCol="features", maxIter=50),
        "Random Forest": RandomForestClassifier(
            labelCol="is_fraud", featuresCol="features",
            numTrees=40, maxDepth=10, seed=SEED),
    }

    results, fitted = [], {}
    for name, classifier in models.items():
        print(f"\nTraining {name}...")
        t0 = time.time()
        model = build_pipeline(classifier).fit(train)
        print(f"  trained in {time.time() - t0:.0f}s, evaluating on the test set...")
        metrics = evaluate(name, model.transform(test))
        results.append(metrics)
        fitted[name] = model

    # ---- Keep the best model (highest AUC-PR, the best metric for rare fraud) ----
    best = max(results, key=lambda r: r["auc_pr"])
    print(f"\nBest model: {best['model']} (AUC-PR {best['auc_pr']})")
    fitted[best["model"]].write().overwrite().save(HDFS_MODEL)
    print(f"Model saved to {HDFS_MODEL}")

    # ---- Feature importance (Random Forest) for the report ----
    # (numeric features come first in the feature vector, so we can match them by position)
    rf_model = fitted["Random Forest"].stages[-1]
    scores = rf_model.featureImportances.toArray()[:len(NUMERIC_COLUMNS)]
    top = sorted(zip(NUMERIC_COLUMNS, scores), key=lambda x: -x[1])[:10]
    print("\nTop 10 most important features (Random Forest):")
    for feature, score in top:
        print(f"  {feature:<22} {score:.4f}")

    with open(METRICS_FILE, "w") as f:
        json.dump({
            "train_rows": n_train, "train_fraud_rows": n_train_fraud, "test_rows": n_test,
            "best_model": best["model"], "results": results,
            "top_features": [{"feature": n, "importance": round(float(s), 4)} for n, s in top],
        }, f, indent=2)
    print(f"\nMetrics saved to {METRICS_FILE}")
    print(f"Total time: {(time.time() - start) / 60:.1f} minutes")
    spark.stop()


if __name__ == "__main__":
    main()
