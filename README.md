Real-Time Financial Transactions Streaming Pipeline Using Azure
Project Overview
This project demonstrates a real-time streaming pipeline for financial transactions using Azure services.
It simulates a continuous stream of transaction data and processes it in real-time to identify potentially fraudulent activity.

Key components include:

Azure Data Factory (simulate streaming ingestion)

Azure Databricks with PySpark Structured Streaming (process data)

Delta Lake on Azure Blob Storage (store outputs)

Fraud detection: transactions over $10,000 are flagged as suspicious.

The pipeline works end-to-end and processes new transactions within seconds.

Architecture Diagram
🔹 Data flow:
Azure Data Factory ➡ Azure Blob Storage (raw) ➡ Azure Databricks Streaming ➡ Azure Blob Storage (processed)

(Architecture Diagram placeholder: insert your image)

Technologies Used
Azure Data Factory (ADF)

Azure Databricks (PySpark)

Azure Blob Storage

Delta Lake

Structured Streaming

How to Run
Set up Azure Blob Storage
Create two containers: raw-transactions, processed-transactions

Configure Databricks Cluster
Mount both Blob containers.

Create ADF Pipeline
Copy simulated transactions into raw-transactions with a trigger.

Run Databricks Streaming Job
Read new files, apply fraud detection logic, write to Delta.

Monitor
Observe results in processed folder + query Delta Lake tables.

Sample Results

Time	Total Transactions	Suspicious Transactions
2025-04-26 13:00	120	3
2025-04-26 13:01	134	2
Future Improvements 🚀
Implement ML models for fraud prediction (instead of thresholding)

Live real-time dashboard (Power BI, Synapse Serverless Pool)

Handle late-arriving transactions (advanced windowing in Spark)

Contact
📩 Feel free to connect with me on LinkedIn:https://www.linkedin.com/in/adarshvijay08/
