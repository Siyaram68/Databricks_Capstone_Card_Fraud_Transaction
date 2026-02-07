# 📘 End-to-End Fraud Detection Lakehouse

## 1. Introduction & Business Problem

**"How do we detect credit card fraud in near real-time without crashing our reporting systems?"**

This project answers that question. We built a modern **Data Lakehouse** on the **Databricks** platform that ingests high-volume raw transaction data, cleans it, and structures it into a "Star Schema" for analytics.

### The Business Goal:
* **Decouple** raw data ingestion from business reporting.
* Provide Executives with a **Dashboard** showing fraud trends by country, category, and risk segment.
* Enable an **Automated Rule Engine** that flags suspicious transactions (e.g., high-value purchases at 3 AM) instantly.

---

## 2. System Architecture (The "Medallion" Model)

We utilized the industry-standard **Medallion Architecture**, which moves data through three quality layers.

### Layer 0: Landing Zone (The Drop-off Point)
* **What it is:** A secure storage container in Azure Data Lake Storage (ADLS Gen2).
* **Role:** This is where the source system (e.g., the credit card payment gateway) dumps the raw CSV files.
* **Key Decision:** We separated this completely from the processing layers. Raw files land here and are effectively "read-only" for the pipeline.

### Layer 1: Bronze (The Raw History)
* **What it is:** The first entry point into the Delta Lake (Databricks).
* **Transformation:** None. The goal is "High Fidelity."
* **Technology:** We used **Auto Loader (`cloudFiles`)**, a smart ingestion tool that detects new files automatically.
* **Schema Evolution:** If the source system adds a new column (e.g., `device_id`), the pipeline automatically adapts without crashing.
* **Quarantine:** Corrupt data isn't deleted; it's saved in a `_rescued_data` column so we never lose information.

### Layer 2: Silver (The Cleanup Crew)
* **What it is:** The filtered, cleaned, and typed version of the data.
* **Transformations Performed:**
    * **Deduplication:** Removed duplicate transactions that might have been sent twice by the source system.
    * **Type Casting:** Converted string dates like `"2025-01-01"` into actual Timestamp objects.
    * **Data Quality:** Dropped the internal `_rescued_data` column, ensuring only valid data moves forward.

### Layer 3: Gold (The Business Model)
* **What it is:** A highly organized "Star Schema" designed for fast queries.
* **Modeling Strategy:**
    * **Fact Table:** `fact_transactions` (The central table with millions of rows).
    * **Dimensions:** `dim_customers` and `dim_merchants` (Context tables).
* **Advanced Logic (SCD Type 2):** I implemented "Slowly Changing Dimensions." If a customer moves from the UK to the US, we keep *both* records—the old one is closed, and the new one is active. This allows us to replay history accurately.
* **Performance:** We applied **Z-Ordering** (a file layout optimization) which makes filtering by Customer ID up to 100x faster.

### Layer 4: BI & Analytics (The Insights)
* **What it is:** Aggregated tables ready for Power BI.
* **The "Rule Engine":** We wrote a simulation script that scans the Gold layer for specific patterns—like cross-border transactions or spending spikes—and tags them as "Suspicious."

---

## 3. Technical Implementation Details

### A. Connecting to Cloud Storage (ADLS)
Instead of using legacy "Mount Points," We used **Unity Catalog Volumes**.
We mapped the ADLS path `abfss://fraud-container@storageaccount.dfs.core.windows.net/` to a local Databricks path `/Volumes/fraud_lake/landing/incoming_data`.
* **Benefit:** This provides secure, governed access to files without hardcoding access keys in the notebook.

### B. The Master Pipeline (Orchestration)
Instead of running 5 separate notebooks manually, we built a **Modular Engineering Pipeline**.

1.  **Functions:** We wrapped the logic of every notebook into a Python function (e.g., `def ingest_bronze():`).
2.  **The Controller:** We created a `Master_Pipeline` notebook that imports these functions.
3.  **Execution:** The Master notebook runs the functions in order:
    * *Step 1:* Run Setup (Create Catalog/Schemas).
    * *Step 2:* Run Bronze Ingestion.
    * *Step 3:* Run Silver Cleaning.
    * *Step 4:* Run Gold Modeling.
    * *Step 5:* Update Dashboard Tables.

If any step fails (e.g., Bronze fails to read a file), the pipeline stops immediately to prevent corrupt data from reaching the dashboard.

---

## 4. Key Code Snippets

### The "Auto Loader" (Bronze Ingestion)
This code ensures we only process *new* files, never the same file twice.

```python
stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/.../schema") # Inference
    .load("/Volumes/fraud_lake/landing/incoming_data")
)

# Trigger(availableNow=True) processes a batch and then shuts down to save money
stream.writeStream.trigger(availableNow=True).toTable("fraud_lake.bronze.transactions")
```
### The "Merge" Logic (Gold Upsert)
This handles updates. If a transaction changes (e.g., marked as fraud later), we update it. If it's new, we insert it.

```python
target.alias("t").merge(
    source.alias("s"),
    "t.transaction_id = s.transaction_id" # Match on ID
).whenMatchedUpdate(
    set = {"fraud_flag": col("s.fraud_flag")} # Update if changed
).whenNotMatchedInsertAll() # Insert if new
.execute()
```
## 5. Challenges & Solutions

| Challenge | Solution |
| :--- | :--- |
| **Schema Drift:** The source system kept adding new columns (e.g., `promo_code`) unexpectedly. | Enabled `.option("mergeSchema", "true")` in Auto Loader, allowing the database to adapt automatically without crashing. |
| **Duplicate Data:** The source sent the same file twice by accident. | Implemented `dropDuplicates(["transaction_id"])` in the Silver layer to guarantee uniqueness. |
| **Slow Queries:** The dashboard became slow when the dataset grew. | Created a **Materialized View** (`bi_daily_sales_summary`) that pre-calculates the totals, allowing Power BI to read 300 rows instead of 3 million. |

---

## 6. How to Run This Project

1.  **Prerequisites:**
    * Access to a **Databricks Workspace**.
    * Access to an **ADLS Gen2** container (or DBFS).

2.  **Upload Data:**
    * Place the raw CSV files into the ADLS container path: `/landing/incoming_data`.

3.  **Run the Orchestrator:**
    * Open the notebook `00_Master_Pipeline_Orchestrator`.
    * Click **"Run All"** to trigger the end-to-end pipeline.

4.  **View Results:**
    * **Fraud Alerts:** Query the table `fraud_lake.gold.suspicious_activity_report` to see flagged transactions.
    * **Dashboard:** Connect Power BI to `fraud_lake.gold.bi_daily_sales_summary` to visualize the metrics.
