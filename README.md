📚 Project Overview
This project simulates a real-world financial transactions streaming system using Azure services. It ingests synthetic transaction data using Azure Data Factory, processes and analyzes the transactions in near real-time using Azure Databricks and Delta Lake, detects suspicious activities, and visualizes metrics like transaction volumes and fraud counts.

🛠️ Technologies Used
Azure Databricks (PySpark Structured Streaming)

Azure Blob Storage (Data Lake Gen2)

Azure Data Factory (Pipelines and Orchestration)

Delta Lake (ACID Storage Layer)

PySpark (ETL Logic)

🛠️ Architecture Diagram

🚀 How to Run
Set up Azure Blob Storage containers: raw-transactions and processed-transactions.

Create Azure Databricks workspace and mount Blob Storage using dbutils.

Build an Auto Loader-based Structured Streaming job to ingest raw transactions.

Apply fraud detection logic (amount > $10,000 flagging).

Write results into Delta Lake tables on Blob Storage.

Analyze real-time aggregated metrics using SQL or notebook queries.

📈 Results
Sample output metrics:


Time Window	Total Txn Count	Suspicious Txn Count	Total Amount
12:00-12:01	50	2	$15000
12:01-12:02	60	1	$10000
