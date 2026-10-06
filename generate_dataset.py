"""
Mobile Money Fraud Detection - Rwanda
Synthetic dataset generator (simple version)

Creates data/mobile_money_transactions.csv (just over 1 GB).
Run:  python generate_dataset.py
"""

import csv
import math
import os
import random
from datetime import datetime, timedelta

random.seed(2026)  # same data every time you run it

# ---------------------------------------------------------------
# 1. Settings
# ---------------------------------------------------------------
OUTPUT_FILE = "data/mobile_money_transactions.csv"
TARGET_SIZE = 1.05 * 1024**3        # stop when the file is just over 1 GB

NUM_ACCOUNTS = 100_000
NUM_AGENTS = 5_000
NUM_MERCHANTS = 3_000

START_TIME = datetime(2024, 1, 1)
AVERAGE_GAP_SECONDS = 8.25          # average time between two transactions

# Rwanda's 30 districts and their provinces
DISTRICTS = {
    "Gasabo": "Kigali", "Kicukiro": "Kigali", "Nyarugenge": "Kigali",
    "Nyanza": "Southern", "Gisagara": "Southern", "Nyaruguru": "Southern",
    "Huye": "Southern", "Nyamagabe": "Southern", "Ruhango": "Southern",
    "Muhanga": "Southern", "Kamonyi": "Southern",
    "Karongi": "Western", "Rutsiro": "Western", "Rubavu": "Western",
    "Nyabihu": "Western", "Ngororero": "Western", "Rusizi": "Western",
    "Nyamasheke": "Western",
    "Rulindo": "Northern", "Gakenke": "Northern", "Musanze": "Northern",
    "Burera": "Northern", "Gicumbi": "Northern",
    "Rwamagana": "Eastern", "Nyagatare": "Eastern", "Gatsibo": "Eastern",
    "Kayonza": "Eastern", "Kirehe": "Eastern", "Ngoma": "Eastern",
    "Bugesera": "Eastern",
}
DISTRICT_NAMES = list(DISTRICTS)
# Kigali districts are busier, so they get a higher weight
DISTRICT_WEIGHTS = [3, 2.5, 2.5] + [1] * 27

# Transaction types, how common each one is, and a typical amount (RWF)
TRANSACTION_TYPES = {
    #  type               share  typical amount
    "CASH_IN":           (17,    20_000),
    "CASH_OUT":          (18,    20_000),
    "P2P_TRANSFER":      (27,    10_000),
    "MERCHANT_PAYMENT":  (14,     5_000),
    "AIRTIME":           (13,     1_000),
    "BILL_PAYMENT":      (7,     10_000),
    "BANK_TRANSFER":     (4,     60_000),
}
TYPE_NAMES = list(TRANSACTION_TYPES)
TYPE_WEIGHTS = [share for share, _ in TRANSACTION_TYPES.values()]

MONEY_OUT_TYPES = ("CASH_OUT", "P2P_TRANSFER", "BANK_TRANSFER")
BILLERS = ["EUCL", "WASAC", "RRA", "CANALPLUS", "STARTIMES"]
BANKS = ["BK", "EQUITY", "IM_BANK", "ECOBANK", "ACCESS"]

COLUMNS = [
    "transaction_id", "timestamp", "account_id", "provider", "customer_age",
    "account_age_days", "home_district", "transaction_type", "channel",
    "amount_rwf", "balance_before", "balance_after", "counterparty_id",
    "tx_district", "tx_province", "is_home_district", "is_new_device",
    "sim_swap_7d", "hour_of_day", "day_of_week", "is_weekend",
    "tx_count_1h", "avg_amount_7d", "is_fraud", "fraud_type",
]


# ---------------------------------------------------------------
# 2. Create customers and agents (they stay the same for every row)
# ---------------------------------------------------------------
def create_accounts():
    accounts = []
    for i in range(1, NUM_ACCOUNTS + 1):
        accounts.append({
            "id": f"ACC{i:06d}",
            "provider": "MTN" if random.random() < 0.7 else "AIRTEL",
            "age": random.randint(16, 75),
            "account_age_days": random.randint(1, 3650),
            "home_district": random.choices(DISTRICT_NAMES, DISTRICT_WEIGHTS)[0],
            "usual_balance": random.lognormvariate(math.log(60_000), 1.0),
        })
    return accounts


def create_agents():
    """Each agent works in one district. About 3% are corrupt."""
    agents_by_district = {d: [] for d in DISTRICT_NAMES}
    for i in range(1, NUM_AGENTS + 1):
        district = random.choices(DISTRICT_NAMES, DISTRICT_WEIGHTS)[0]
        agents_by_district[district].append({
            "id": f"AGT{i:05d}",
            "corrupt": random.random() < 0.03,
        })
    return agents_by_district


# ---------------------------------------------------------------
# 3. Build one transaction
# ---------------------------------------------------------------
def pick_counterparty(tx_type, account, agent):
    if tx_type in ("CASH_IN", "CASH_OUT"):
        return agent["id"]
    if tx_type == "P2P_TRANSFER":
        return f"ACC{random.randint(1, NUM_ACCOUNTS):06d}"
    if tx_type == "MERCHANT_PAYMENT":
        return f"MER{random.randint(1, NUM_MERCHANTS):05d}"
    if tx_type == "AIRTIME":
        return f"TEL_{account['provider']}"
    if tx_type == "BILL_PAYMENT":
        return f"BILL_{random.choice(BILLERS)}"
    return f"BANK_{random.choice(BANKS)}"


def make_transaction(tx_number, timestamp, accounts, agents_by_district):
    account = random.choice(accounts)
    tx_type = random.choices(TYPE_NAMES, TYPE_WEIGHTS)[0]
    hour = timestamp.hour
    is_night = hour >= 23 or hour <= 4

    # Amount: most are near the typical amount, a few are much bigger
    typical_amount = TRANSACTION_TYPES[tx_type][1]
    amount = round(random.lognormvariate(math.log(typical_amount), 0.9), -1)
    amount = max(100, amount)

    # Where it happens: usually in the customer's home district
    if random.random() < 0.85:
        tx_district = account["home_district"]
    else:
        tx_district = random.choices(DISTRICT_NAMES, DISTRICT_WEIGHTS)[0]
    is_home = tx_district == account["home_district"]

    agent = random.choice(agents_by_district[tx_district])
    channel = "AGENT" if tx_type in ("CASH_IN", "CASH_OUT") else random.choice(["USSD", "USSD", "APP"])

    # SIM swap and new phone (a SIM swap usually means a new phone too)
    sim_swap = random.random() < 0.012
    new_device = random.random() < (0.75 if sim_swap else 0.05)

    # Balance before and after
    usual = account["usual_balance"] * random.uniform(0.5, 1.5)
    if tx_type == "CASH_IN":
        balance_before = usual
        balance_after = balance_before + amount
    else:
        # Sometimes (more often at night) the account is almost emptied
        drain_chance = 0.20 if is_night else 0.04
        if tx_type in MONEY_OUT_TYPES and random.random() < drain_chance:
            balance_before = amount / random.uniform(0.9, 1.0)
        else:
            balance_before = max(usual, amount * random.uniform(1.2, 4.0))
        balance_after = balance_before - amount

    # Recent behaviour of this account
    tx_count_1h = random.choices([1, 2, 3, 4, 5, 6, 7], [60, 22, 9, 4, 3, 1, 1])[0]
    if random.random() < 0.03:        # unusual spike compared to normal spending
        avg_amount_7d = amount * random.uniform(0.05, 0.18)
    else:
        avg_amount_7d = amount * random.uniform(0.5, 1.5)

    tx = {
        "transaction_id": f"MM{tx_number:010d}",
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "account_id": account["id"],
        "provider": account["provider"],
        "customer_age": account["age"],
        "account_age_days": account["account_age_days"],
        "home_district": account["home_district"],
        "transaction_type": tx_type,
        "channel": channel,
        "amount_rwf": int(amount),
        "balance_before": int(balance_before),
        "balance_after": int(balance_after),
        "counterparty_id": pick_counterparty(tx_type, account, agent),
        "tx_district": tx_district,
        "tx_province": DISTRICTS[tx_district],
        "is_home_district": int(is_home),
        "is_new_device": int(new_device),
        "sim_swap_7d": int(sim_swap),
        "hour_of_day": hour,
        "day_of_week": timestamp.weekday(),          # 0 = Monday
        "is_weekend": int(timestamp.weekday() >= 5),
        "tx_count_1h": tx_count_1h,
        "avg_amount_7d": int(avg_amount_7d),
    }

    fraud_type = check_fraud(tx, is_night, agent)
    tx["is_fraud"] = int(fraud_type != "none")
    tx["fraud_type"] = fraud_type
    return tx


# ---------------------------------------------------------------
# 4. Fraud rules
#    Each rule has a chance (not 100%) so the model must learn
#    patterns, not memorise exact rules.
# ---------------------------------------------------------------
def check_fraud(tx, is_night, agent):
    money_out = tx["transaction_type"] in MONEY_OUT_TYPES
    emptied = tx["balance_after"] < 0.1 * tx["balance_before"]

    rules = [
        # (fraud name,          condition,                                          chance)
        ("sim_swap_takeover",    tx["sim_swap_7d"] and tx["is_new_device"] and money_out, 0.60),
        ("night_account_drain",  is_night and emptied and money_out,                      0.30),
        ("agent_collusion",      agent["corrupt"] and tx["transaction_type"] == "CASH_OUT"
                                 and not tx["is_home_district"],                          0.50),
        ("new_account_large_tx", tx["account_age_days"] < 60 and tx["amount_rwf"] > 80_000
                                 and money_out,                                           0.30),
        ("rapid_p2p_transfers",  tx["tx_count_1h"] >= 5
                                 and tx["transaction_type"] == "P2P_TRANSFER",            0.15),
        ("amount_spike",         tx["amount_rwf"] > 5 * tx["avg_amount_7d"],              0.08),
    ]
    for name, condition, chance in rules:
        if condition and random.random() < chance:
            return name

    if random.random() < 0.002:       # a little random fraud (real life is noisy)
        return "other"
    return "none"


# ---------------------------------------------------------------
# 5. Main: write transactions in time order until the file is > 1 GB
# ---------------------------------------------------------------
def main():
    os.makedirs("data", exist_ok=True)
    print("Creating accounts and agents...")
    accounts = create_accounts()
    agents_by_district = create_agents()

    timestamp = START_TIME
    tx_number = 0
    fraud_count = 0

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()

        while True:
            # Move time forward. Nights are quieter, so gaps are longer.
            gap = AVERAGE_GAP_SECONDS * (4 if timestamp.hour <= 5 else 1)
            timestamp += timedelta(seconds=random.expovariate(1 / gap))

            tx_number += 1
            tx = make_transaction(tx_number, timestamp, accounts, agents_by_district)
            writer.writerow(tx)
            fraud_count += tx["is_fraud"]

            # Every 500,000 rows, check progress and file size
            if tx_number % 500_000 == 0:
                f.flush()
                size = os.path.getsize(OUTPUT_FILE)
                print(f"{tx_number:,} rows | {size / 1024**3:.2f} GB | up to {timestamp:%Y-%m-%d}")
                if size >= TARGET_SIZE:
                    break

    print("\nDone!")
    print(f"File       : {OUTPUT_FILE}")
    print(f"Rows       : {tx_number:,}")
    print(f"Fraud rows : {fraud_count:,} ({fraud_count / tx_number:.2%})")


if __name__ == "__main__":
    main()
