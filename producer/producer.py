# Kafka producer: reads transactions from HDFS and sends them to Kafka
# one by one, like live mobile money payments

import os
import time
from hdfs import InsecureClient
from kafka import KafkaProducer

HDFS_FILE = "/momo/raw/mobile_money_transactions.csv"
TOPIC = "momo-transactions"
START_DATE = "2026-01-01"   # only send Jan-May 2026 (the model never saw these)
PER_SECOND = 20             # transactions sent per second
PROGRESS_FILE = "/app/producer/last_sent.txt"   # remembers where we stopped

hdfs_client = InsecureClient("http://namenode:9870", user="hadoop")
producer = KafkaProducer(bootstrap_servers="kafka:29092")

# continue after the last transaction sent in the previous run
last_id = ""
if os.path.exists(PROGRESS_FILE):
    last_id = open(PROGRESS_FILE).read().strip()
    print("Continuing after", last_id)

print("Reading", HDFS_FILE, "from HDFS")
print("Skipping old transactions (takes 1-2 minutes)...")

sent = 0
try:
    with hdfs_client.read(HDFS_FILE, encoding="utf-8", delimiter="\n") as lines:
        for line in lines:
            fields = line.split(",")

            # skip the header, empty lines and transactions already sent
            if len(fields) < 3 or fields[0] == "transaction_id":
                continue
            if fields[1] < START_DATE or fields[0] <= last_id:
                continue

            # the account id is used as the key, so all transactions
            # of one account go to the same Kafka partition
            producer.send(TOPIC, key=fields[2].encode(), value=line.encode())
            sent += 1
            last_id = fields[0]

            if sent % 100 == 0:
                print(f"sent {sent} transactions (last: {fields[0]} at {fields[1]})")
                open(PROGRESS_FILE, "w").write(last_id)

            time.sleep(1 / PER_SECOND)

except KeyboardInterrupt:
    print("Stopped by user")

producer.flush()
open(PROGRESS_FILE, "w").write(last_id)
print("Total sent:", sent, "| last transaction:", last_id)
