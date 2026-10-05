from datetime import timedelta

from django.db.models import Count, F, Q, Sum
from django.db.models.functions import Floor, TruncMinute
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone

from .models import FeatureImportance, ModelMetric, Prediction


def dashboard(request):
    # the page itself; the numbers are loaded by JavaScript from stats()
    return render(request, "fraud/dashboard.html")


def percent(part, whole):
    return round(part / whole * 100, 1) if whole else 0


def stats(request):
    tx = Prediction.objects.all()
    flagged = tx.filter(predicted_fraud=1)

    # main figures and the live confusion matrix
    total = tx.count()
    n_flagged = flagged.count()
    true_pos = flagged.filter(actual_fraud=1).count()
    false_neg = tx.filter(predicted_fraud=0, actual_fraud=1).count()
    false_pos = n_flagged - true_pos
    true_neg = total - n_flagged - false_neg
    money_at_risk = int(flagged.aggregate(s=Sum("amount_rwf"))["s"] or 0)

    # when did Spark save the last batch?
    last = tx.order_by("-processed_at").values_list("processed_at", flat=True).first()
    seconds_ago = int((timezone.now() - last).total_seconds()) if last else None

    # transactions scored per minute during the last 30 minutes of activity
    per_minute = []
    if last:
        recent = (
            tx.filter(processed_at__gte=last - timedelta(minutes=30))
            .annotate(minute=TruncMinute("processed_at"))
            .values("minute")
            .annotate(n=Count("transaction_id"), alerts=Sum("predicted_fraud"))
            .order_by("minute")
        )
        per_minute = [
            {"minute": r["minute"].strftime("%H:%M"), "n": r["n"], "alerts": int(r["alerts"] or 0)}
            for r in recent
        ]

    # alerts per district for the map
    districts = {
        r["tx_district"]: {"n": r["n"], "alerts": int(r["alerts"] or 0)}
        for r in tx.values("tx_district").annotate(n=Count("transaction_id"), alerts=Sum("predicted_fraud"))
    }

    # how the fraud scores are spread (10 buckets: 0-10%, 10-20%, ...)
    risk_buckets = [0] * 10
    for r in tx.annotate(b=Floor(F("fraud_probability") * 10)).values("b").annotate(n=Count("transaction_id")):
        risk_buckets[min(int(r["b"]), 9)] += r["n"]

    by_hour = {r["hour_of_day"]: r["n"] for r in flagged.values("hour_of_day").annotate(n=Count("transaction_id"))}
    by_type = list(flagged.values("transaction_type").annotate(n=Count("transaction_id")).order_by("-n"))

    # how many real frauds of each pattern the model caught
    patterns = []
    for r in (
        tx.filter(actual_fraud=1)
        .values("fraud_type")
        .annotate(total=Count("transaction_id"), caught=Count("transaction_id", filter=Q(predicted_fraud=1)))
        .order_by("-total")
    ):
        patterns.append({"name": r["fraud_type"], "total": r["total"], "caught": r["caught"],
                         "rate": percent(r["caught"], r["total"])})

    feed = [
        {
            "id": a.transaction_id,
            "time": a.event_time.strftime("%d %b %H:%M"),
            "account": a.account_id,
            "type": a.transaction_type,
            "amount": a.amount_rwf,
            "district": a.tx_district,
            "probability": round(a.fraud_probability * 100),
            "actual": a.actual_fraud,
            "fraud_type": a.fraud_type,
        }
        for a in flagged.order_by("-event_time")[:12]
    ]

    return JsonResponse({
        "total": total,
        "flagged": n_flagged,
        "share_flagged": percent(n_flagged, total),
        "money_at_risk": money_at_risk,
        "precision": percent(true_pos, n_flagged),
        "recall": percent(true_pos, true_pos + false_neg),
        "confusion": {"tp": true_pos, "fp": false_pos, "fn": false_neg, "tn": true_neg},
        "seconds_ago": seconds_ago,
        "per_minute": per_minute,
        "districts": districts,
        "risk_buckets": risk_buckets,
        "by_hour": [by_hour.get(h, 0) for h in range(24)],
        "by_type": by_type,
        "patterns": patterns,
        "feed": feed,
        "models": list(ModelMetric.objects.order_by("-is_best").values()),
        "features": list(FeatureImportance.objects.order_by("-importance").values()),
    })