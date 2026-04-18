# E-commerce Analytics Using Apache Spark for Distributed Computing

## 1. Project Overview

This project is a distributed analytics pipeline built with **Apache Spark (Scala)** to analyze large-scale e-commerce data. It processes clickstream events, orders, and product catalog data to generate business insights that are commonly used by data engineering and analytics teams.

The pipeline demonstrates how distributed computing can be used for:

- sessionization of user behavior,
- funnel conversion analysis,
- attribution modeling,
- anomaly detection,
- KPI computation,
- and exporting analytics outputs to BigQuery.

The project is designed for batch analytics and can be executed locally (for demonstration/testing) or on a Spark cluster such as **Google Cloud Dataproc** (recommended for full-scale execution).

---

## 2. What Problem This Project Solves

In real e-commerce systems, raw event logs and transactional records are too large and too unstructured for direct decision-making.

This project transforms raw logs into structured analytical outputs so stakeholders can answer questions such as:

- How users move through the purchase funnel,
- Which referrer/source should get credit for orders,
- Whether conversion behavior dropped abnormally,
- What products are selling the most,
- Core business KPIs (orders, revenue, AOV, conversion rate).

---

## 3. Dataset and Input Files

Input files are expected in a folder path referred to as DATA_PATH.

Default file names:

- events.csv
- orders.csv
- catalog.csv

### 3.1 events.csv

Expected columns:

- user_id
- timestamp
- event_type
- product_id
- session_id
- device
- referrer

### 3.2 orders.csv

Expected columns:

- order_id
- user_id
- timestamp
- total_amount

### 3.3 catalog.csv

Expected columns:

- product_id
- category
- brand
- price

Note: Input files in this project use tab-separated values (TSV format) even though the extension is .csv.

---

## 4. Processing Modules

The Spark application is organized into modular components:

- DataLoader: Reads and parses input files.
- Sessionization: Creates derived user sessions using inactivity timeout logic.
- FunnelAnalysis: Computes views -> add_to_cart -> purchase stage metrics.
- Attribution: Performs last-touch attribution for orders.
- AnomalyDetection: Flags significant deviations in add_to_purchase conversion trends.
- Metrics: Computes KPI outputs and grouped summaries.
- MainApp: End-to-end orchestration and BigQuery export.

---

## 5. Outputs Produced

The application prints analytics in the console and writes result tables to BigQuery.

### Console outputs

- Total Events
- Total Orders
- Total Revenue
- Average Order Value
- Conversion Rate
- Events by Type
- Top Selling Products
- Funnel summary
- Attribution summary
- Anomaly summary

### BigQuery outputs

The following tables are written (with configurable prefix):

- {prefix}_kpi_snapshot
- {prefix}_events_by_type
- {prefix}_top_products
- {prefix}_funnel
- {prefix}_attribution
- {prefix}_anomalies

---

## 6. Technology Stack

- Scala 2.12.18
- Apache Spark 3.5.0 (Core + SQL)
- SBT 1.x build tool
- sbt-assembly plugin
- BigQuery connector (at runtime in cluster environment)

---

## 7. Prerequisites

Before running the project, make sure the following are available:

1. Java 11 (recommended for this setup)
2. SBT installed
3. Spark-compatible runtime (local or Dataproc)
4. Input data files in data/ or GCS path
5. (Optional but recommended) BigQuery dataset and write permissions

Important compatibility note:

- Java 23 may fail with Spark initialization in this project.
- Use Java 11 to avoid this issue.

---

## 8. How to Run the Project (Local)

This mode is useful for demonstration and verification from GitHub clone.

### Step 1: Clone repository

```bash
git clone https://github.com/VINAY163581/E-commerce_Analytics_Using_Apache_Spark_for_Distributed_Computing.git
cd E-commerce_Analytics_Using_Apache_Spark_for_Distributed_Computing
```

### Step 2: Ensure Java 11 is active

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

### Step 3: Use local data path

```bash
export DATA_PATH="data"
```

### Step 4: Run Spark job with local master

```bash
sbt '-Dspark.master=local[*]' "runMain com.analytics.MainApp"
```

Notes:

- The project contains large synthetic datasets, so execution can take significant time locally.
- For full-scale processing, Dataproc cluster execution is recommended.

---

## 9. How to Run the Project on Dataproc (Recommended)

This is the preferred mode for distributed execution in cloud environment.

### Step 1: Build assembly JAR

```bash
sbt clean assembly
```

Expected output JAR path (example):

- target/scala-2.12/spark-ecommerce-analytics-assembly-1.0.jar

### Step 2: Upload JAR and input data to GCS

```bash
gsutil cp target/scala-2.12/*assembly*.jar gs://<your-bucket>/jars/
gsutil cp data/*.csv gs://<your-bucket>/data/
```

### Step 3: Submit Spark job

```bash
gcloud dataproc jobs submit spark \
	--cluster=<your-cluster-name> \
	--region=<your-region> \
	--class=com.analytics.MainApp \
	--jars=gs://<your-bucket>/jars/<assembly-jar-name> \
	--properties=spark.jars.packages=com.google.cloud.spark:spark-3.5-bigquery:0.42.1
```

### Step 4: Pass environment variables when submitting (important)

MainApp reads configuration from environment variables. A common Dataproc pattern is to pass them through Spark properties:

```bash
gcloud dataproc jobs submit spark \
	--cluster=<your-cluster-name> \
	--region=<your-region> \
	--class=com.analytics.MainApp \
	--jars=gs://<your-bucket>/jars/<assembly-jar-name> \
	--properties=spark.jars.packages=com.google.cloud.spark:spark-3.5-bigquery:0.42.1,spark.yarn.appMasterEnv.DATA_PATH=gs://<your-bucket>/data,spark.executorEnv.DATA_PATH=gs://<your-bucket>/data,spark.yarn.appMasterEnv.BQ_PROJECT_ID=<your-project>,spark.yarn.appMasterEnv.BQ_DATASET=<your-dataset>,spark.yarn.appMasterEnv.BQ_TABLE_PREFIX=ecommerce,spark.yarn.appMasterEnv.TEMP_GCS_BUCKET=<your-temp-bucket>
```

Environment variables used by this project:

- DATA_PATH (example: gs://<your-bucket>/data)
- BQ_PROJECT_ID
- BQ_DATASET
- BQ_TABLE_PREFIX
- TEMP_GCS_BUCKET

If not provided, the app uses default values coded in MainApp.

---

## 10. Environment Variables Reference

- DATA_PATH: Base path for events.csv, orders.csv, catalog.csv
- BQ_PROJECT_ID: BigQuery project id
- BQ_DATASET: BigQuery dataset name
- BQ_TABLE_PREFIX: Prefix for all output tables
- TEMP_GCS_BUCKET: Temporary bucket used by BigQuery connector

---

## 11. Project Structure

```text
.
|- build.sbt
|- README.md
|- data/
|  |- catalog.csv
|  |- events.csv
|  |- orders.csv
|- scripts/
|  |- generate_dataproc_data.py
|- src/main/scala/com/analytics/
|  |- MainApp.scala
|  |- SparkSessionBuilder.scala
|  |- data/DataLoader.scala
|  |- session/Sessionization.scala
|  |- funnel/FunnelAnalysis.scala
|  |- attribution/Attribution.scala
|  |- anomaly/AnomalyDetection.scala
|  |- util/Metrics.scala
```

---

## 12. Troubleshooting

### Issue: Spark fails with Java security/subject errors

Cause: Running with unsupported/newer Java runtime.

Fix: Switch to Java 11.

### Issue: A master URL must be set

Cause: Running locally without spark.master.

Fix: Run with:

```bash
sbt '-Dspark.master=local[*]' "runMain com.analytics.MainApp"
```

### Issue: BigQuery write fails

Possible causes:

- Missing BigQuery connector package,
- Invalid project/dataset/bucket values,
- Missing IAM permissions for BigQuery or GCS.

Fix:

- Ensure connector is added during job submission.
- Verify environment variables and access permissions.

---

## 13. Academic Context

This project is submitted as part of coursework on **Distributed Software Systems / Distributed Computing**, demonstrating:

- distributed data ingestion,
- parallel transformations with Spark SQL/DataFrame APIs,
- scalable analytics pipeline design,
- cloud-compatible execution and analytical output generation.

---

## 14. Quick Start (for Professor)

If you want to test quickly after cloning:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
export PATH="$JAVA_HOME/bin:$PATH"
export DATA_PATH="data"
sbt '-Dspark.master=local[*]' "runMain com.analytics.MainApp"
```

This runs the end-to-end analytics pipeline and prints KPI + analytics outputs in terminal.