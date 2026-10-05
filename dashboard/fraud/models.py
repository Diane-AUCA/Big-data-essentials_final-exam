from django.db import models


# These tables were created by setup_database.py and are filled by Spark,
# so Django only reads them (managed = False).

class Prediction(models.Model):
    transaction_id = models.CharField(max_length=20, primary_key=True)
    event_time = models.DateTimeField()
    processed_at = models.DateTimeField()
    account_id = models.CharField(max_length=12)
    provider = models.CharField(max_length=10)
    transaction_type = models.CharField(max_length=20)
    channel = models.CharField(max_length=10)
    amount_rwf = models.IntegerField()
    balance_before = models.IntegerField()
    balance_after = models.IntegerField()
    counterparty_id = models.CharField(max_length=20)
    tx_district = models.CharField(max_length=20)
    tx_province = models.CharField(max_length=20)
    hour_of_day = models.IntegerField()
    fraud_probability = models.FloatField()
    predicted_fraud = models.IntegerField()
    actual_fraud = models.IntegerField()
    fraud_type = models.CharField(max_length=30)

    class Meta:
        managed = False
        db_table = "predictions"


class ModelMetric(models.Model):
    model_name = models.CharField(max_length=40, primary_key=True)
    auc_roc = models.FloatField()
    auc_pr = models.FloatField()
    precision_score = models.FloatField()
    recall_score = models.FloatField()
    f1_score = models.FloatField()
    accuracy = models.FloatField()
    is_best = models.IntegerField()

    class Meta:
        managed = False
        db_table = "model_metrics"


class FeatureImportance(models.Model):
    feature = models.CharField(max_length=40, primary_key=True)
    importance = models.FloatField()

    class Meta:
        managed = False
        db_table = "feature_importance"
