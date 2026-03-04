# Distributed Batch Data Platform (Netflix Architecture Clone)

## 📌 Context & Problem Statement
To build highly personalized recommendation engines, machine learning models require meticulously cleaned, aggregated, and trustworthy data. This project implements a production-grade batch data processing pipeline using the **Medallion Architecture** (Bronze, Silver, Gold) to process the **MovieLens 25M dataset**. 

The core engineering challenge addressed here is **Data Quality at Scale**. Bad data corrupts downstream ML models silently. To prevent this, I implemented the **Write-Audit-Publish (WAP)** pattern using Apache Iceberg and Great Expectations to ensure zero bad data reaches the presentation layer.

## 🏗️ Architecture
*(Insert your architecture diagram here)*
`![Architecture Diagram](architecture.png)`

## 🛠️ Tech Stack & Engineering Choices
* **Compute:** PySpark (Designed for AWS EMR Serverless to handle 25M+ rows efficiently).
* **Storage Format:** Apache Iceberg (Selected over Parquet/Delta for its superior native branching capabilities, which are essential for the WAP pattern, and seamless schema evolution).
* **Data Lake:** Amazon S3 & AWS Glue Catalog.
* **Data Quality:** Great Expectations (In-memory DataFrame validation).
* **Testing:** Pytest (Test-Driven Development for isolated transformation logic).
* **Environment:** GitHub Codespaces (100% cloud-native development).

## 🛡️ Data Quality Approach: The WAP Pattern
Instead of writing data directly to production tables and risking corruption if a job fails mid-flight, this pipeline uses Iceberg's branching features:
1. **Write:** Cleaned data is appended to a hidden Iceberg `audit_staging` branch.
2. **Audit:** Great Expectations reads the *physical* Parquet files from the staging branch and runs a strict suite of checks (e.g., ratings must be between 0.5-5.0, user-movie combinations must be strictly unique).
3. **Publish:** If and only if the audit passes, an atomic `fast_forward` operation advances the `main` production branch pointer. If it fails, the pipeline halts and production remains untouched.

## 🚀 How to Run Locally (Testing Suite)
Because this pipeline is designed for AWS EMR, the transformation logic is heavily decoupled from the I/O operations. You can run the entire test suite locally without needing an AWS account or a local Spark installation.

1. Clone the repository and open it in GitHub Codespaces.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   sudo apt-get install openjdk-17-jre openjdk-17-jdk
   export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64