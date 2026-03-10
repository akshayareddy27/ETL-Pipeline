# Data Cleaning and Transformation Pipeline (Milestone 2)

## 1. Overview

In this milestone, an ETL (Extract–Transform–Load) pipeline was developed to process options market data from Kaggle.
The pipeline automates the process of downloading, cleaning, transforming, and storing financial options data for further analysis.

The pipeline is orchestrated using Apache Airflow and processes SPY options chain data to derive useful market insights.

---

## 2. Objectives

The main objectives of this milestone were:

- Develop a data cleaning workflow for raw options data
- Implement data transformation using Python (Pandas)
- Extract useful market indicators from the dataset
- Build a reliable and automated ETL pipeline
- Store processed data in a structured database

---

## 3. Dataset Description

The dataset used in this project is from Kaggle:

Dataset Name:

`option-chain-field-price-prediction`

It contains SPY options chain market data, which includes information about call and put options for different strike prices.

Important Columns in the Dataset
| Column | Description |
|------------------|--------------------------------------------------|
| QUOTE_UNIXTIME | Timestamp of the option quote |
| UNDERLYING_LAST | Price of the underlying asset (SPY) |
| STRIKE | Option strike price |
| C_BID / C_ASK | Call option bid and ask prices |
| P_BID / P_ASK | Put option bid and ask prices |
| C_VOLUME | Call option trading volume |
| P_VOLUME | Put option trading volume |
| C_IV | Call option implied volatility |
| P_IV | Put option implied volatility |
| Option Greeks | Risk metrics (Delta, Gamma, Vega, Theta) |

This dataset allows analysis of options market activity and trading behavior

---

## 4. Pipeline Architecture

The pipeline follows the ETL architecture:

```
Kaggle Dataset
       │
       ▼
Extract Data
       │
       ▼
Clean Raw Data
       │
       ▼
Transform Data
       │
       ▼
Generate Market Insights
       │
       ▼
Store in SQLite Database

```

The workflow is automated using Apache Airflow DAG.

---

## 5. Extract Phase

The extraction stage downloads the dataset from Kaggle using the Kaggle API.

Key steps:

1. Authenticate with Kaggle API
2. Download the dataset
3. Store it in the project data directory

Output location:

` /opt/airflow/dags/etl_pipeline/data`

---

## 6. Data Cleaning Phase

The raw dataset may contain duplicates or missing values.
To ensure data quality, the following cleaning steps were implemented.

### Duplicate Removal

Duplicate records are removed to avoid repeated data.

### Missing Value Removal

Rows containing missing values are removed to maintain data integrity.

### Dataset Validation

The pipeline verifies:

- Dataset is not empty
- Required columns exist

These checks ensure the pipeline does not process corrupted data.

---

## 7. Data Transformation

After cleaning the dataset, several transformations were applied to extract useful insights.

### 7.1 Timestamp Conversion

The dataset contains time in UNIX format.
It was converted to a readable datetime format.

Purpose:

- Enables time-based analysis
- Improves data readability

---

### 7.2 At-The-Money (ATM) Implied Volatility

ATM options are those whose strike price is closest to the underlying asset price.

Steps performed:

1. Calculate distance between strike price and underlying price
2. Identify the closest strike
3. Compute ATM implied volatility

Metric created:

`ATM_IV = (Call_IV + Put_IV) / 2`

Purpose:

- Measures expected market volatility
- Widely used in options trading analysis

---

### 7.3 Put-Call Ratio (PCR)

Put-Call Ratio measures market sentiment.

Formula:
`PCR = Total Put Volume / Total Call Volume`

Interpretation:
PCR Value Market Sentiment

| Ratio Value | Market Sentiment  |
| ----------- | ----------------- |
| >1          | Bearish Sentiment |
| <1          | Bullish Sentiment |

Purpose:

- Helps understand trader expectations in the market.

---

### 7.4 Most Active Option Strikes

Trading activity was analyzed by calculating total option volume.

Formula:
` Total Volume = Call Volume + Put Volume`

The pipeline identifies the top 10 most active strike prices.

Purpose:

- Identify key trading levels
- Detect highly liquid options

These strikes often act as support or resistance levels

---

## 8. Load Phase

The processed data is stored in a SQLite database.

Database file:

`stocks.db`

Two tables are created.

**Table 1: cleaned_data**

Contains the fully cleaned dataset.

Purpose:

- Preserve processed raw data
- Allow further analysis.

**Table 2: atm_data**

Contains only ATM option records.

Purpose:

- Focused dataset for volatility analysis.

Chunk loading was used to efficiently insert large datasets.

---

## 9. Pipeline Orchestration with Airflow

The pipeline is automated using Apache Airflow.

Three tasks were created:

1. Extract Data
2. Transform Data
3. Load Data

Task dependency:

```text
extract_data → transform_data → load_data
```

This ensures the pipeline runs in the correct order.

---

## 10. Pipeline Testing and Validation

Several validation checks were implemented.

### Dataset Validation

Ensures dataset is not empty.

### Schema Validation

Verifies required columns exist.

### Cleaning Verification

Tracks number of removed rows.

### Error Handling

Prevents division errors when calculating Put-Call Ratio.

> These checks ensure the pipeline runs reliably.

---

## 11. Outputs Generated

The pipeline generates the following outputs:

### Files Generated

```
cleaned_full.csv
cleaned.csv
stocks.db
```

### Database Tables created

```
cleaned_data
atm_data
```

### Market Insights

- ATM implied volatility
- Put-call ratio
- Most active option strikes

---

## 12. Technologies Used

| Technology     | Purpose                          |
| -------------- | -------------------------------- |
| Python         | Core programming language        |
| Pandas         | Data cleaning and transformation |
| Apache Airflow | Pipeline orchestration           |
| Kaggle API     | Dataset extraction               |
| SQLite         | Data storage                     |

---

## 13. Key Achievements

This milestone successfully delivered:

- Automated ETL pipeline
- Cleaned financial dataset
- Extracted market insights
- Structured database storage
- Workflow orchestration using Apache Airflow

