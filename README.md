# Financial Data Reconciliation & Reporting Pipeline

## 📌 Project Overview
This repository demonstrates a modern, automated data pipeline designed for a CFO / Finance Data Enablement team. It simulates the extraction, reconciliation, and database loading of financial ledger data for regulatory reporting. 

**Goal:** Migrate manual, legacy processes (like SAS/Cron or Excel/VBA) into a robust, automated Python/Airflow architecture.

## 🚀 Tech Stack Used
*   **Language:** Python 3.9+
*   **Data Transformation:** Pandas, Numpy (handling missing data, vectorization for performance)
*   **Database Integration:** SQLAlchemy, SQL (SQLite used for local demo)
*   **Orchestration:** Apache Airflow (replaces legacy Cron scheduling with DAGs and failure alerting)

## 💼 Business Value
1. **Automated Reconciliation:** Uses Pandas to automatically identify breaks (amount mismatches, missing transactions) between internal bank ledgers and external clearing house data.
2. **Auditability:** Implements logging and error handling, ensuring data lineage is maintained for regulatory compliance.
3. **Scalability:** Moves away from local Cron jobs to Airflow, providing dependency management and automatic retries.

## 📂 Repository Structure
* `dags/` - Contains the Airflow DAG (`recon_pipeline.py`) that schedules and orchestrates the ETL.
* `src/` - Core Python logic. `transform.py` handles Pandas reconciliation logic. `load.py` handles SQLAlchemy database insertion.
* `data/` - Contains mock CSVs for internal ledgers and external clearing data.


THE FINAL END‑TO‑END FLOW (the one sentence version)
You write a DAG file → place it in the Airflow DAGs folder → Airflow automatically loads it → the scheduler triggers it at the defined Cron schedule → workers run your Python ETL tasks → sensors wait for files → transform/load/validate steps run → alerts fire if anything fails → results go to the database → analysts review the output.
