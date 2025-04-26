# Real-Time Financial Transactions Streaming Pipeline Using Azure
![Built with Azure Databricks](https://img.shields.io/badge/Built%20with-Azure%20Databricks-orange)

## Project Overview

This project demonstrates a real-time **streaming pipeline for financial transactions** using Azure services.  
It simulates a continuous stream of transaction data and processes it in real-time to identify potentially fraudulent activity.

Key components:
- **Azure Data Factory** to simulate real-time ingestion
- **Azure Databricks** with **PySpark Structured Streaming** to process data
- **Azure Blob Storage** and **Delta Lake** for scalable, reliable storage

Fraud detection logic flags transactions over **$10,000** as suspicious.

The pipeline showcases real-time analytics, processing new transactions within seconds.

---

## Architecture Diagram

![Architecture Diagram](pipeline%20image.png)

*Azure Data Factory ➔ Blob Storage ➔ Databricks (Auto Loader + Structured Streaming) ➔ Delta Lake.*

---

## Technologies Used

- Azure Data Factory (ADF)
- Azure Databricks (PySpark)
- Azure Blob Storage
- Delta Lake
- Structured Streaming

---

## How to Run

1. **Set up Azure Storage**: Create containers `raw-transactions` and `processed-transactions`.
2. **Configure Databricks Cluster**: Mount Blob containers using dbutils or ABFS path.
3. **Create ADF Pipeline**: Simulate new transactions every 5 minutes using Copy Activity + Tumbling Trigger.
4. **Run the Databricks Streaming Notebook**: 
   - Ingest data using Auto Loader
   - Apply fraud detection
   - Write results to Delta Lake
5. **Monitor Outputs**: 
   - Check ADF pipeline runs
   - Check processed Delta files
   - Query transaction and fraud metrics in Databricks

---

## Results

| Time (Minute)        | Total Transactions | Suspicious Transactions |
|----------------------|--------------------|-------------------------|
| 2025-04-26 13:00:00  | 120                | 3                       |
| 2025-04-26 13:05:00  | 134                | 2                       |

Processed data is available for real-time querying and fraud analysis.

---

## Future Improvements 🚀

- **Advanced Fraud Detection**: Integrate ML models instead of static thresholds.
- **Real-Time Dashboards**: Build live reports in Power BI or Synapse Serverless.
- **Complex Streaming Patterns**: Handle late-arriving events, watermarking, sliding windows.

---

## Contact

Feel free to connect with me on [LinkedIn](https://www.linkedin.com/in/adarshvijay08/)!

