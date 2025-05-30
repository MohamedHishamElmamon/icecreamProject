# Ice Cream Truck Analytics: End-to-End Azure Data Pipeline
![original-0fbcd5a0e7618354139b80160fb606c1](https://github.com/user-attachments/assets/1ac77c0b-7283-4e20-8255-11db7e627948)

> **Important Note**  
> There are many additional enhancements and automations that could further improve this architecture. However, due to the time constraints of this assignment, the primary focus was on core ingestion, transformation, and incremental loading best practices. Moreover, there are various ways to build an integrated analytics solution on Azure (including Synapse or Microsoft Fabric). I specifically chose **Azure Data Factory (ADF)** plus **Azure Databricks** to demonstrate my ability to integrate multiple Azure components and handle enterprise-level complexities. This choice does increase setup challenges (e.g., connecting ADF to a Databricks cluster, configuring ADLS access, and dealing with self-hosted Integration Runtimes for ODBC connections), but it showcases broad Azure data engineering skills—exactly what this challenge required.

---

## Table of Contents
1. [Introduction & Business Scenario](#introduction--business-scenario)  
2. [Why ADF + Databricks Instead of Synapse or Fabric?](#why-adf--databricks-instead-of-synapse-or-fabric)  
3. [Architecture Overview](#architecture-overview)  
4. [Solution Components & Tools Used](#solution-components--tools-used)  
5. [Folder & Repo Structure](#folder--repo-structure)  
6. [Data Sources & Input Files](#data-sources--input-files)  
7. [Incremental & Partitioned Data Processing (Bronze → Silver → Gold)](#incremental--partitioned-data-processing-bronze--silver--gold)  
   1. [Bronze Layer](#bronze-layer)  
   2. [Silver Layer](#silver-layer)  
   3. [Gold Layer](#gold-layer)  
   4. [Error Handling & Logging](#error-handling--logging)  
   5. [Watermark Logic](#watermark-logic)  
8. [Performance Optimizations](#performance-optimizations)  
9. [Power BI Semantic View & Key Calculations](#power-bi-semantic-view--key-calculations)  
10. [CI/CD Process](#cicd-process)  
11. [Monitoring & Alerts](#monitoring--alerts)  
12. [Conclusion](#conclusion)

---

## 1. Introduction & Business Scenario

This project tackles a **scalable data pipeline** for a fictional client aiming to rent out ice-cream trucks across Europe. The key questions include:

- **Weather Connection**: To what extent does weather influence ice cream sales?  
- **Best Country for Main Location**: Which country yields the highest ROI?  
- **Revenue vs. Rent**: Identify periods where monthly sales are at least *3 times* the truck’s rental cost.  
- **ROI Analysis**: Factor in Big Mac Index/cost-of-living data to see where profitability is strongest.  
- **Optimal Country Pattern**: Determine the truck’s best seasonal route across Europe.

We ingest data from multiple sources (weather APIs, Azure SQL DB, CSV files in Blob Storage), clean and normalize it using **Azure Databricks**, store it as **Delta** in **Azure Data Lake Storage (ADLS) Gen2**, then surface dashboards in **Power BI**. The pipeline uses watermark-based **incremental loading** for performance and cost savings.

![1](https://github.com/user-attachments/assets/0e550921-e57b-4d8d-9764-55df6db4423e)

---

## 2. Why ADF + Databricks Instead of Synapse or Fabric?

I have experience with **all three** approaches:

1. **ADF + Azure Databricks** (what I used here)  
2. **Azure Synapse Analytics**, which integrates pipelines, Spark, and SQL pools in a single service  
3. **Microsoft Fabric**, a newly introduced platform that unifies Data Factory, Data Lakehouse, and Power BI

Choosing **ADF + Databricks** highlights my ability to:
- Integrate multiple Azure services in a more “modular” approach.  
- Overcome the extra complexities (e.g., connecting ADF to a Databricks cluster, hooking up Databricks SQL Warehouses, self-hosted integration runtime for ODBC connections, etc.).  
- Showcase robust data engineering skills while meeting the reliability and scalability requirements.

**Why not just use Synapse or Fabric?**  
- **Time & Familiarity**: While I’m familiar with Synapse and Fabric—and they can simplify certain tasks (e.g., a single environment for pipelines + Spark)—I wanted to demonstrate flexibility with separate services.  
- **Enterprise Realism**: Many organizations already have ADF orchestrations and Databricks for advanced Spark. This scenario mirrors real-world setups where multiple systems must coordinate.  
- **Simplicity vs. Depth**: Synapse or Fabric might have been faster to set up, but ADF + Databricks reveals deeper skill sets.

---

## 3. Architecture Overview

A high-level data pipeline flow:

![Assignment](https://github.com/user-attachments/assets/704b2044-093b-49e0-818c-7aa54ba31f64)

1. **Data Sources**  
   - **Azure Blob**: CSVs, JSON files.  
   - **Weather API**: Real or mocked data.  
   - **Azure SQL Database**: Additional structured data (e.g., sales transactions).  

2. **Ingestion**  
   - **Azure Data Factory** pipelines copy source data into **Bronze** layer in ADLS.

3. **Transformations**  
   - **Azure Databricks** for incremental data processing, applying watermarks for each load stage:  
     - *Bronze → Silver*: Cleansing, standardization.  
     - *Silver → Gold*: SCD Type 2 merges for dimension tables, upserts for fact tables.

4. **Storage**  
   - **ADLS Gen2** in Delta format across three layers (Bronze, Silver, Gold).  
   - **metadata_config** holds watermark and error log tables.
![watermark](https://github.com/user-attachments/assets/825a5c1c-5704-4c99-900f-a7aa702b16bb)

5. **Analytics**  
   - **Power BI** connects to a single semantic view: `vw_icecream_powerbi_dataset`.

6. **Monitoring & Alerts**  
   - **Databricks** logs errors in a dedicated table (`etl_error_logs`).

7. **CI/CD**  
   - **GitHub** for version control (ADF JSON, notebooks, DDL, Power BI).  
   - **Azure Pipelines** to automate builds/deployments.

---

## 4. Solution Components & Tools Used

1. **Azure Data Factory (ADF)**  
   - Pipeline orchestration, data movement from Blob to ADLS, scheduling.  
2. **Azure Databricks**  
   - Primary ETL engine (PySpark, SQL).  
3. **Delta Lake on ADLS Gen2**  
   - ACID transactions, partitioning, time-travel for data.  
4. **Azure Monitor & Log Analytics**  
   - Observability for pipeline runs and alerting on failures.  
5. **Power BI**  
   - Dashboards and advanced DAX analytics for weather-sales correlation, ROI, rent viability.  
6. **GitHub + Azure Pipelines**  
   - Source control, continuous integration, and automated deployment to Azure.

---

## 5. Folder & Repo Structure

```plaintext
icecreamProject/
├── ADF resources/
│   ├── dataset/
│   ├── integrationRuntime/
│   ├── linkedService/
│   └── pipeline/
├── Architecture/
│   └── ... (diagrams)
├── Databricks/
│   ├── config/
│   │   ├── etl_error_notebook.py
│   │   └── update_config_tables.py
│   ├── silver/
│   │   ├── Silver_Data_Processing.py
│   │   └── silver_ddl_scripts.sql
│   ├── gold/
│   │   ├── gold_data_processing.py
│   │   └── gold_ddl_scripts.sql
│   └── semantic/
│       └── vw_icecream_powerbi_dataset.sql
├── Powerbi/
│   └── Europe Ice cream Dashboard.pbix
├── Raw Data/
│   ├── countries.csv
│   ├── country_index.csv
│   ├── ...
│   └── transactions.csv
├── azure-pipelines.yml
└── README.md  (this file)
```

- **ADF resources/**: Data Factory definitions (datasets, pipelines, ARM templates).  
- **Databricks/config/**: Utility scripts (error logging, config updates).  
- **Databricks/silver** & **Databricks/gold**: Main ETL notebooks and DDL statements.  
- **Powerbi/**: Final `.pbix` dashboard.  
- **Raw Data/**: Sample CSV data representing possible input.  
- **azure-pipelines.yml**: Azure DevOps pipeline for CI/CD.

---

## 6. Data Sources & Input Files

1. **Weather** (API/CSV)  
   - Stored in `bronze/WeatherAPI/` then refined to `silver.weather_api`.  
2. **Sales Transactions** (e.g., `transactions.csv`, Azure SQL DB)  
   - Land in `bronze/TransactionDB/`. Aggregated daily to `silver.sales_transactions`.  
3. **Country Index** (`country_index.csv`)  
   - Processed into `silver.country_index`.  
4. **Rental Prices** (`rental_prices.csv`)  
   - Monthly columns unpivoted into normal rows → `silver.rental_prices_monthly`.  
5. **Products Lookup** (`products_lookup.csv`)  
   - Basic transformations, stored in `silver.products_lookup`.

The final step merges data into:

- **Gold dimensions**: `dim_product`, `dim_country`, `dim_date`.  
- **Gold fact**: `fact_sales`.

---

## 7. Incremental & Partitioned Data Processing (Bronze → Silver → Gold)

### 7.1 Bronze Layer
- **Minimal transformation**—just raw ingestion to ADLS.

### 7.2 Silver Layer
- **Cleaning & Standardization**:
  - Null checks, data type conversions, filtering out-of-range values (e.g., invalid temperatures).  
  - [Silver_Data_Processing.py](../Databricks/silver/Silver_Data_Processing.py) covers each domain (weather, sales, etc.).  
  - Writes partitioned Delta tables (e.g., partition by `processing_date`).

### 7.3 Gold Layer
- **Analytics-Ready**:
  - [gold_data_processing.py](../Databricks/gold/gold_data_processing.py) merges data via SCD Type 2 logic.  
  - **dim_product** tracks changing product prices over time.  
  - **dim_country** tracks changing country indices.  
  - **fact_sales** includes daily sales, weather metrics, rental prices, etc.
![Untitled](https://github.com/user-attachments/assets/fbf3e829-8a87-48ae-b773-9b5ad376fdfe)

### 7.4 Error Handling & Logging
- **etl_error_notebook.py**:
  - Called on exceptions.  
  - Logs to `etl_error_logs` with source, message, pipeline info, and timestamp.
![error_etl](https://github.com/user-attachments/assets/18e6dddf-1d30-4138-8ed1-ff768869de2c)

### 7.5 Watermark Logic
- **metadata_config.*_watermark**:
  - Each pipeline references the latest `watermark_value`.  
  - Only processes rows where `processing_timestamp > watermark_value`.  
  - Updates watermark after a successful run.
- API Weather Pipeline
![ADF 1](https://github.com/user-attachments/assets/a9deeb47-a3fd-4ef9-ba98-ccbdb51d7562)
- Azure SQL Sales Transactions Pipeline
- ![ADF 2](https://github.com/user-attachments/assets/fa571e92-0dad-4c84-b90a-81cb003068ba)
- Main Pipeline (End-To-End)
![ADF 3](https://github.com/user-attachments/assets/2afbdf67-9b08-4c19-98f7-0935ad09bb93)

---

## 8. Performance Optimizations

1. **Incremental Loads**  
   - Saves compute by processing new data only.  
2. **Partition Pruning**  
   - Filters on partition columns (`processing_date`, `sales_date`) reduce scanned data.  
3. **SCD Type 2**  
   - MERGE changes only for updated records.  
4. **Caching**  
   - Could be used for repeated transformations in Databricks.

---

## 9. Power BI Semantic View & Key Calculations

1. **Semantic View**: [vw_icecream_powerbi_dataset.sql](../Databricks/semantic/vw_icecream_powerbi_dataset.sql)  
   - Joins `fact_sales` with `dim_date`, `dim_product`, `dim_country`.  
   - Derives season, day_type, temperature_category, etc.

2. **Key DAX Measures**  
   - **Weather_Sales_Correlation**: Compare average sales on warm/no-rain days vs. all days.  
   - **Strategic_Positioning_Score**: Weighted metric combining revenue, weather advantage, risk, etc.  
   - **Revenue_to_Rent_Ratio**: Tracks 3× monthly rent threshold.  
   - **Annual_ROI_Percentage**: `(AnnualRevenue - AnnualRent) / AnnualRent * 100`.  
   - **Optimal_Route_Score**: RANKX for best seasonal route.

3. **Visuals**  
   - Weather vs. Sales correlation scatter.  
   - “Risk vs. Opportunity” matrix.  
   - Rent Viability heatmap.  
   - ROI matrix with Big Mac Index adjustments.

*(More advanced visuals or measures were possible, but time constraints limited scope.)*

![1](https://github.com/user-attachments/assets/78243436-1daa-4fed-8e7c-eb5cd5164fc3)\
![2](https://github.com/user-attachments/assets/6637e1ae-40c7-4bed-bee4-35005fb28cea)
![3](https://github.com/user-attachments/assets/8c76fbc3-1b99-44e7-a99c-df06df7f1868)

---

## 10. CI/CD Process

1. **GitHub**  
   - Central repo for ADF JSON, notebooks, scripts, and `.pbix`.
2. **Azure Pipelines** (`azure-pipelines.yml`)  
   - Deploys ADF pipelines and Databricks notebooks.  
   - Optionally runs tests (e.g., data validations, lint checks) prior to production.
3. **Release Flow**  
   - Developer commits → PR → Merge to main → Pipeline triggers → ADF & Databricks updated in Azure.

---

## 11. Monitoring & Alerts

- **Azure Data Factory**  
  - Monitor tab for pipeline success/failure, plus configurable alerts.  
- **Databricks**  
  - Notebook logs, cluster event logs, and job statuses.  
  - On notebook errors, calls `etl_error_notebook.py` → logs into `etl_error_logs`.

---

## 12. Conclusion

This solution provides a **real-world, end-to-end** data and analytics pipeline using **Azure Data Factory**, **Azure Databricks**, **Delta Lake**, and **Power BI**:

- **Why ADF + Databricks**: Chosen to demonstrate advanced Azure data engineering capabilities (rather than a single integrated service like Synapse or Fabric).  
- **Incremental, Partitioned Structure**: Ensures fast performance and lower costs, processing only new data.  
- **SCD Type 2**: Captures historical dimension changes.  
- **Robust Error Handling**: Central `etl_error_logs` table and watermark logic for re-runs.  
- **Comprehensive CI/CD**: GitHub for code versioning, Azure Pipelines for automated deployments.  
- **Interactive Analytics**: Power BI dashboards and DAX measures reveal weather-sales correlation, ROI, rental viability, etc.

While there are many other potential enhancements—more advanced transformations, deeper Power BI visuals, or an even more automated metadata-driven ingestion—this design meets the core assignment goals. It showcases a fully functioning pipeline built on best practices in data loading, transformation, and analytics on Azure.
