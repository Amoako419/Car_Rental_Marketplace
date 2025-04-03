# Car Rental Analytics Pipeline

<p align="center">
    <img src="images/architecture_diagram (1).jpg" alt="The architecture diagram" width="100%" />
</p>

## Overview

The **Car Rental Analytics Pipeline** is a Spark-based data processing pipeline designed to calculate key performance indicators (KPIs) for a car rental business. The pipeline processes raw transactional data stored in Amazon S3, computes KPIs such as revenue, transaction counts, and rental durations, and saves the results back to S3 in Parquet format. This project leverages AWS services like S3, EMR, and Glue to build a scalable and automated analytics solution.

The pipeline is orchestrated using **AWS Step Functions**, which ensures seamless coordination between various tasks such as launching an EMR cluster, running Spark jobs, triggering Glue crawlers, and performing post-processing queries with Athena.

---

## Features

- **Scalable Data Processing**: Uses Apache Spark for distributed computation.
- **Automated Workflow**: Integrated with AWS Step Functions for orchestration.
- **KPI Calculations**:
  - Revenue per location.
  - Total transactions per location.
  - Transaction amount statistics (average, maximum, minimum).
  - Unique vehicles used per location.
  - Rental duration metrics (total, average).
- **Data Validation**: Ensures data quality by validating schemas, checking for null values, and verifying timestamps.
- **Output Storage**: Saves processed KPIs in Parquet format to an S3 bucket for further analysis or visualization.


---

## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/Amoako419/Car_Rental_Marketplace.git
cd car-rental-analytics
```

### 2. Configure AWS Credentials
Ensure your AWS credentials are configured using the AWS CLI:
```bash
aws configure
```
Provide your `AWS Access Key`, `Secret Key`, region (e.g., `us-west-2`), and default output format (e.g., `json`).

### 3. Prepare Input Data
Upload the raw datasets (`locations.csv`, `rental_transactions.csv`, `users.csv`, `vehicles.csv`) to the S3 bucket path specified in the code:
```
s3://car-rental-bucket-125/land-folder/raw_data/
```

### 4. Deploy the EMR Cluster
Use the provided AWS CLI command or Step Function workflow to create an EMR cluster with Spark enabled.

---

## Usage

### Explore Data Locally
Use the `notebooks/eda.ipynb` notebook to perform exploratory data analysis (EDA) and compute KPIs locally using PySpark. Open the notebook with Jupyter:
```bash
jupyter notebook notebooks/eda.ipynb
```

---

## KPI Calculations

The pipeline calculates the following KPIs:

### Location and Vehicle Performance:
- **Total Revenue per Location**: Total revenue generated at each pickup location.
- **Total Transactions per Location**: Number of rental transactions at each pickup location.
- **Average Transaction Amount per Location**: Average value of transactions at each location.
- **Max/Min Transaction Amount per Location**: Maximum and minimum transaction amounts at each location.
- **Unique Vehicles Used per Location**: Count of unique vehicles rented at each pickup location.
- **Rental Duration Metrics by Vehicle Type**: Total and average rental duration (in hours) grouped by vehicle type.

### User and Transaction Metrics:
- **Total Daily Transactions and Revenue**: Total number of transactions and revenue generated per day.
- **Average Transaction Value**: Average value of all transactions.
- **User Engagement Metrics**:
  - Total transactions per user.
  - Total revenue generated per user.
- **Max and Min Spending per User**: Maximum and minimum spending by individual users.
- **Total Rental Hours per User**: Total rental duration (in hours) for each user.

---

## AWS Step Function Workflow

The **AWS Step Function** orchestrates the entire pipeline, ensuring tasks are executed in the correct order and handling failures gracefully. Below is a breakdown of the steps involved:

### 1. **Start EMR Cluster**
   - Launches an EMR cluster in a specified VPC and subnet.
   - Configures the cluster with necessary instance groups (Master and Core nodes) and IAM roles.
   - Waits for the cluster to become available before proceeding.

### 2. **Run Spark Job 1**
   - Executes the first Spark job (`spark_job_1.py`) on the EMR cluster.
   - Computes KPIs related to **location and vehicle performance** (e.g., revenue per location, unique vehicles used).

### 3. **Run Spark Job 2**
   - Executes the second Spark job (`spark_job_2.py`) on the EMR cluster.
   - Computes KPIs related to **user and transaction metrics** (e.g., total transactions per user, average transaction value).

### 4. **Trigger Glue Crawler**
   - Triggers an AWS Glue crawler to catalog the processed KPI data stored in S3.
   - Ensures the data is indexed and queryable in the Glue Data Catalog.

### 5. **Wait for Crawler Completion**
   - Waits for the Glue crawler to complete its run.
   - Ensures the data is fully cataloged before proceeding to the next step.

### 6. **Parallel Athena Queries**
   - Executes multiple SQL queries in parallel using **Amazon Athena** to analyze the processed KPI data.
   - Example queries:
     - Retrieve top 10 locations by revenue.
     - Calculate total transactions across all locations.
     - Summarize total revenue generated.

### 7. **Terminate EMR Cluster**
   - Terminates the EMR cluster after all Spark jobs are completed.
   - Ensures cost optimization by shutting down resources when they are no longer needed.

### 8. **SNS Publish**
   - Sends a notification via **Amazon SNS** to notify stakeholders about the completion of the pipeline.
   - Includes details about the success or failure of the pipeline.

---

## Insights from Athena Queries

After the pipeline processes the data and stores it in S3, **Amazon Athena** is used to extract insights from the processed datasets. Below are the key queries and their insights:

### 1. **Top 5 Days with Maximum Revenue**
```sql
SELECT rental_date, max(total_revenue) as total_revenue 
FROM user_transaction_kpis_parquet 
GROUP BY rental_date 
ORDER BY total_revenue DESC 
LIMIT 5;
```

- **Purpose**: Identifies the top 5 days with the highest total revenue.
- **Dataset**: `user_transaction_kpis_parquet`
  - Contains aggregated transaction data grouped by date (`rental_date`) and location.
  - Columns: `rental_date`, `total_revenue`.
- **Insight**:
  - Helps identify peak business days (e.g., weekends, holidays) where revenue is highest.
  - Useful for planning marketing campaigns or promotions around these high-revenue periods.

---

### 2. **Top 5 Users by Total Spending**
```sql
SELECT * 
FROM user_metrics_parquet 
ORDER BY total_spent DESC 
LIMIT 5;
```

- **Purpose**: Identifies the top 5 users who spent the most money on rentals.
- **Dataset**: `user_metrics_parquet`
  - Contains user-level metrics such as total spending, total transactions, etc.
  - Columns: `user_id`, `total_spent`, etc.
- **Insight**:
  - Highlights high-value customers who contribute significantly to revenue.
  - Useful for customer retention strategies, loyalty programs, or personalized offers.

---

### 3. **Top 5 Locations by Maximum Transaction Amount**
```sql
SELECT * 
FROM location_kpis_parquet 
ORDER BY max_transaction DESC 
LIMIT 5;
```

- **Purpose**: Identifies the top 5 locations with the highest single transaction amounts.
- **Dataset**: `location_kpis_parquet`
  - Contains location-level metrics such as total revenue, transaction counts, and maximum/minimum transaction amounts.
  - Columns: `pickup_location`, `max_transaction`, etc.
- **Insight**:
  - Highlights locations where high-value transactions occur.
  - Useful for understanding regional demand patterns and optimizing resource allocation (e.g., premium vehicles in high-demand areas).

---

## Error Handling and Logging

The pipeline includes robust error handling and logging mechanisms:

- **Error Handling**:
  - Validates input data schemas and checks for missing or null values.
  - Handles exceptions during Spark operations and logs detailed error messages.
- **Logging**:
  - Logs are written to both the console and a local file for debugging.
  - Includes timestamps, log levels (INFO, WARNING, ERROR), and contextual messages.

---

## Project Structure

The project is organized as follows:

```
car-rental-analytics/
├── spark_jobs/                  # Folder containing Spark job scripts
│   ├── spark_job_1.py           # First Spark job script
│   ├── spark_job_2.py           # Second Spark job script
├── notebooks/                   # Jupyter notebooks for EDA and local KPI computations
│   └── eda.ipynb                # EDA and KPI computations using PySpark
├── data/                        # Local data folder   
├── s3_scripts/                  # Scripts to load data and Spark jobs into S3
│   ├── load_data_to_s3.py       # Script to upload local data to S3
│   ├── load_spark_jobs_to_s3.py # Script to upload Spark jobs to S3
├── requirements.txt             # Python dependencies
├── .env                         # Environment variables for AWS credentials
├── README.md                    # Project documentation
└── images/                      # Images used in the documentation
```



 
