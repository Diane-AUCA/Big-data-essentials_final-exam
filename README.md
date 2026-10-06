# Real-Time Mobile Money Fraud Detection for Rwanda

Big Data Essentials final exam.

## Group members

| No. | Name | Student number |
|---|---|---|
| 1 | MUSABYIMANA Diane | 101218 |
| 2 | UGIZWENAYO Divine | 101191 |
| 3 | TUMUKUNDE Sandra | 101483 |
| 4 | TWAGIRAYEZU Emmanuel | 101214 |
| 5 | EMERIMANA Edwin Kennedy | 101205 |
| 6 | MANIRAGUHA Jean de Dieu | 10118 |

## Links

- Dashboard (hosted on AWS EC2): http://13.51.200.99
- Dashboard code: https://github.com/Diane-AUCA/Big-data-essentials_final-exam


The dashboard shows "Live" only while the pipeline is running. Otherwise it shows "Stream paused" and the data already stored in the database.

## What the project does

The system detects fraud in mobile money transactions (MTN MoMo and Airtel Money) as they happen. A synthetic dataset of 7.5 million transactions (1.06 GB, 1 January 2024 to 31 May 2026, about 1.3% fraud) is stored in HDFS. A Kafka producer reads it from HDFS and publishes it like live payments. A Spark Structured Streaming job cleans each batch, scores every transaction with a Random Forest trained in Spark MLlib, and saves the result to a MySQL database on AWS RDS. A Django dashboard reads the database and refreshes every 5 seconds.

The data is synthetic because real mobile money data is confidential. The fraud patterns were defined by the group, so the results show that the system works end to end and do not describe real fraud levels.

## How it works

```
HDFS --> Kafka producer --> Kafka topic --> Spark Streaming (clean + predict) --> AWS RDS (MySQL) --> Django dashboard
  |                                                ^
  +--> Spark MLlib training (train_model.py) ------+  (the saved model is loaded from HDFS)
```

The model is trained on 2024 and 2025 and tested on January to May 2026. Only those unseen months are streamed.

## Versions

| Tool | Version |
|---|---|
| Hadoop (HDFS) | 3.3.6 |
| Apache Kafka | 3.7.0 (KRaft mode) |
| Apache Spark / PySpark | 3.5.1 (Scala 2.12) |
| Java | OpenJDK 17 (inside the Spark container) |
| Python | 3.11 (Spark container), 3.12.7 (laptop), 3.14.4 (EC2 server) |
| MySQL on AWS RDS | 8.4.9 |
| Django | 5.2.17 |
| gunicorn | 26.2.0 |
| Spark Kafka connector | org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 |

Python libraries in the Spark container: pyspark 3.5.1, numpy 1.26.4, pandas 2.2.2, kafka-python 2.0.2, hdfs 2.7.3, pymysql 1.1.1, cryptography 43.0.3 (see `docker/spark/Dockerfile`).
Python libraries for the dashboard: see `dashboard/requirements.txt` (installed versions: Django 5.2.17, PyMySQL 1.2.3, cryptography 50.0.2, python-dotenv 1.2.4, gunicorn 26.2.0).

## Folder structure

```
.
|-- docker-compose.yml          starts HDFS (namenode, datanode), Kafka and Spark
|-- .env.example                database settings to copy to .env (the real .env is not included)
|-- .gitignore
|-- generate_dataset.py         creates data/mobile_money_transactions.csv
|-- docker/
|   |-- hadoop.env              HDFS settings
|   `-- spark/Dockerfile        PySpark 3.5.1 and Java 17 image
|-- spark/
|   |-- train_model.py          trains Logistic Regression and Random Forest, saves the best to HDFS
|   |-- setup_database.py       creates the tables in RDS and loads the model results
|   `-- streaming.py            Structured Streaming: clean, predict, save to RDS
|-- producer/
|   `-- producer.py             reads HDFS and publishes to Kafka
`-- dashboard/
    |-- manage.py
    |-- requirements.txt
    |-- momo_dashboard/         settings.py, urls.py
    `-- fraud/                  models.py, views.py, templates/fraud/dashboard.html
```

## Requirements

- Docker Desktop with Docker Compose, and at least 8 GB of RAM
- Python 3.10 or newer (only for generating the data and running the dashboard on your own computer)
- A MySQL 8 database that the containers can reach. The project used AWS RDS (MySQL 8.4) with public access and a security group that allows port 3306 from your IP address.

## Setup and run

Run the commands from the project folder.

1. **Database settings.** Copy `.env.example` to `.env` and fill in the RDS endpoint, user and password. Never share or upload the `.env` file.

2. **Create the data** (about 3 minutes, needs only standard Python):
   ```
   python generate_dataset.py
   ```

3. **Start the containers** (the first time takes 5 to 15 minutes):
   ```
   docker compose up -d --build
   docker compose ps
   ```

4. **Upload the data to HDFS and check it:**
   ```
   docker exec namenode hdfs dfs -mkdir -p /momo/raw
   docker exec namenode hdfs dfs -put /data/mobile_money_transactions.csv /momo/raw/
   docker exec namenode hdfs fsck /momo
   ```
   The last line must say HEALTHY. The HDFS web page is at http://localhost:9870.

5. **Create the Kafka topic:**
   ```
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic momo-transactions --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1
   ```

6. **Train the model** (about 7 minutes). It saves the model to HDFS and the scores to `spark/model_metrics.json`:
   ```
   docker exec spark python /app/spark/train_model.py
   ```

7. **Create the database tables and test the connection:**
   ```
   docker exec spark python /app/spark/setup_database.py
   ```

8. **Start the streaming job** (leave this terminal open):
   ```
   docker exec -it spark python /app/spark/streaming.py
   ```

9. **Start the producer** in a second terminal. It skips the training period for 1 to 2 minutes before it starts sending:
   ```
   docker exec -it spark python /app/producer/producer.py
   ```
   Every 5 seconds the streaming terminal prints a line such as `batch 81: 100 saved to RDS, 2 flagged as fraud`.

10. **Open the dashboard** at http://13.51.200.99 or run it on your own computer (see below).

To stop: press Ctrl+C in the producer terminal, then in the streaming terminal, then run `docker compose stop`. Use `stop` and not `down`, because `down` deletes the data stored in HDFS.

## Running the dashboard on your own computer

```
cd dashboard
pip install -r requirements.txt
python manage.py migrate
python manage.py runserver
```

Then open http://127.0.0.1:8000. The dashboard reads the database settings from the `.env` file in the project folder.

## How the dashboard is hosted

The dashboard runs on an AWS EC2 server (t3.micro, Ubuntu Server 26.04 LTS) in the same region as the database. The code is downloaded from GitHub, installed in a virtual environment, and started by a systemd service that runs gunicorn on port 80. On the server the `.env` file also sets `DJANGO_DEBUG=0`, `ALLOWED_HOSTS` and a random `SECRET_KEY`. To update the site: edit the file on GitHub, then on the server run `git pull` and `sudo systemctl restart momo-dashboard`.

The database firewall allows port 3306 only from the developer's IP address and from the web server's security group. If the connection from your computer times out, your internet connection has a new IP address: open the database security group in the AWS console, edit your rule, choose "My IP" and save. The web server's rule does not change.

## Main results

- Random Forest on 1,281,702 unseen transactions: AUC-ROC 0.91, AUC-PR 0.353, precision 29.8%, recall 60.1%.
- Live stream of 279,605 transactions: 7,307 flagged (2.6%), precision 30.5%, recall 60.7%. The streamed months are the same as the test months, so these figures agree with the test results by construction.
- The strongest signals are an account nearly emptied, a recent SIM swap, a SIM swap on a new phone, many transactions within an hour and night-time activity.

## Limitations

- The data is synthetic.
- Everything runs on one machine: one datanode, one Kafka broker, Spark in local mode.
- The agent is not a model input, so corrupt agents are not detected.
- The dashboard uses plain HTTP and has no login.
- The server has no fixed IP address, so its public address changes if the instance is stopped and started.
