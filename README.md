# Credit Risk Assessment Pipeline (Databricks + DLT + AWS)

## 📌 Project Overview

This project implements an **end-to-end ETL pipeline for credit risk assessment** using **Databricks, PySpark, Delta Lake, and AWS services**.

The pipeline processes raw financial datasets and transforms them into **analytics-ready datasets** using the **Medallion Architecture (Bronze → Silver → Gold)**.

The final output enables **credit risk analysis and financial insights** by generating key features such as:

* Loan-to-income ratio
* Risk score and risk category
* Default probability indicators
* Credit behavior insights
* Loan performance analytics

---

## 📊 Dataset

### Dataset Source

Financial/Credit Risk dataset (simulated real-world loan & borrower data)

### Datasets Used

* `applicant_profiles` → Borrower demographic details
* `loan_details` → Loan information
* `credit_history` → Credit scores & repayment behavior
* `credit_applications` → Loan application records
* `economic_indicators` → External economic data

These datasets simulate a **real-world banking/credit risk system**.

---

## 🏗️ Lakehouse Architecture

The pipeline integrates **Databricks + Delta Live Tables + AWS services**.

[![
](https://github.com/supriya-1109/Credit_Risk_Assessment_Pipeline/blob/main/assests/ARCHITECTURE_1773861870064.jpeg?raw=true)
### End-to-End Flow

```
AWS S3 → Databricks (DLT Pipeline) → Delta Tables → Analytics Layer
```

---

## 🥇 Medallion Architecture Layers

### 🔹 Bronze Layer (Raw Data)

#### Purpose

* Store raw data as-is
* Preserve lineage
* Enable traceability

#### Tables

```
bronze_applicant_profiles
bronze_loan_details
bronze_credit_history
bronze_credit_applications
bronze_economic_indicators
```

#### Operations

* Raw data ingestion from S3
* Schema enforcement
* Metadata tracking

---

### 🔹 Silver Layer (Cleaned Data)

#### Purpose

* Data cleaning and standardization
* Data validation and enrichment
* Feature preparation

#### Transformations

* Remove duplicates
* Handle missing/null values
* Data type casting
* Standardize categorical fields
* Median imputation
* Data quality flags & error handling

#### Output Tables

```
silver_applicant_profiles
silver_loan_details
silver_credit_history
silver_credit_applications
silver_economic_indicators
```

---

### 🔹 Gold Layer (Analytics Data)

#### Purpose

* Build **business-ready data models**
* Implement **Star Schema (Fact + Dimension Tables)**
* Generate analytics features

---

## Data Modelling (Star Schema)

### 📌 Dimension Tables

```
dim_borrower   → borrower details
dim_credit     → credit score & history
dim_property   → property information
dim_loan       → loan attributes
dim_time       → time dimension
dim_location   → regional data
```

---

### 📌 Fact Table

```
fact_loan_application
```
![
](https://github.com/supriya-1109/Credit_Risk_Assessment_Pipeline/blob/main/assests/STAR%20SCHEMA_1773861977573.png?raw=true)
#### Metrics Included

* Loan amount
* Interest rate
* Loan-to-value (LTV)
* Loan-to-income ratio
* Debt-to-income ratio
* Previous default flag
* Risk score
* Risk category

---

### 📌 Final Analytics Table

```
credit_catalog.analytics.risk_features
```

## ⚙️ Orchestration (Databricks + Airflow)

The pipeline is implemented using **Delta Live Tables (DLT)**.
![
](https://github.com/supriya-1109/Credit_Risk_Assessment_Pipeline/blob/main/assests/image_1773893287936.png?raw=true)
### Pipeline Stages
1. Bronze Ingestion
2. Silver Transformation
3. Gold Aggregation

### Execution

* Incremental processing
* Automated pipeline execution
* Fault-tolerant design

---

## ✅ Data Quality Checks

Implemented validations include:

* Null value checks
* Duplicate detection
* Range validation (credit score, interest rate)
* Schema validation
* Data quality flags

Monitoring tools:

* Databricks logs
* DLT expectations
* Pipeline event logs

---

## 📁 Project Folder Structure

```
Credit_Risk_Assessment_Pipeline
│
├── raw_datasets
│
├── development
│   ├── bronze
│   ├── silver
│   ├── gold
│   └── airflow_dag
│
├── testing
│   └── Pytest
│
├── dashboards
│   └── dashboard_images
│
│── assets
│
│──Credit Risk Assessment Pipeline.ppt
│
└── README.md
```


---

## 🛠️ Technologies Used

* Python
* PySpark
* Databricks
* Delta Lake
* Delta Live Tables (DLT)
* AWS S3
* AWS Glue
* Apache Airflow
* Git & GitHub

---

## 🚀 Git Integration

Clone the repository:

```
git clone https://github.com/supriya-1109/Credit_Risk_Assessment_Pipeline.git
cd Credit_Risk_Assessment_Pipeline
```

Install dependencies:

```
pip install -r requirements.txt
```

---

## ▶️ Running the Pipeline

Run using Databricks DLT pipeline:

* Create pipeline in Databricks
* Attach notebook (gold + silver + bronze)
* Run pipeline

### Execution Stages

* Bronze ingestion
* Silver transformation
* Gold feature engineering

---

## 📊 Analytics & Dashboards

The Gold layer supports dashboards such as:

### 📌 Risk Distribution Dashboard

* High / Medium / Low risk segmentation
![
](https://github.com/supriya-1109/Credit_Risk_Assessment_Pipeline/blob/main/assests/LOAN%20VS%20AMOUNT_1773948601661.png?raw=true)
### 📌 Loan Performance Dashboard
* Loan amount trends
* Default patterns

![](https://github.com/samba2226/CreditRiskAssessmentPipeline/blob/main/dashboards/dashboard_images/Loan%20Performance%20Dashboard.png?raw=true)

### 📌 Credit Score Analysis

* Credit score distribution
* Risk correlation

![](https://github.com/samba2226/CreditRiskAssessmentPipeline/blob/main/dashboards/dashboard_images/Credit%20Score%20Distribution.png?raw=true)

### 📌 Regional Risk Insights

* Region-wise risk analysis

![](https://github.com/samba2226/CreditRiskAssessmentPipeline/blob/main/dashboards/dashboard_images/Region-wise%20Risk%20Distribution.png?raw=true)


---

## 💡 Business Insights Generated

### 📊 Risk Analysis

Identify high-risk borrowers likely to default

### 💰 Loan Performance

Analyze loan repayment trends

### 📉 Default Prediction

Estimate probability of default

### 🌍 Regional Insights

Identify high-risk regions

### 🧾 Credit Behavior

Understand borrower financial patterns

---

## 🔮 Future Enhancements

* Real-time streaming pipeline
* Machine learning risk prediction models
* Advanced BI dashboards
* Automated anomaly detection
* Data observability tool
