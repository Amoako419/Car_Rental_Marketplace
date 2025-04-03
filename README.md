# Car Rental Analytics Pipeline

<p align="center">
    <img src="images/architecture_diagram (1).jpg" alt="The architecture diagram" width="100%" />
</p>

## Overview

The **Car Rental Analytics Pipeline** is a Spark-based data processing pipeline designed to calculate key performance indicators (KPIs) for a car rental business. The pipeline processes raw transactional data stored in Amazon S3, computes KPIs such as revenue, transaction counts, and rental durations, and saves the results back to S3 in Parquet format. This project leverages AWS services like S3, EMR, and Glue to build a scalable and automated analytics solution.

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

## Prerequisites

Before running the pipeline, ensure you have the following:

1. **AWS Account**: Access to AWS services like S3, EMR, and Glue.
2. **AWS CLI**: Installed and configured with appropriate permissions.
3. **Spark Environment**: An EMR cluster or local Spark installation.
4. **Python**: Python 3.9 installed.
5. **Dependencies**:
   - PySpark (`pip install pyspark`)
   - Boto3 (`pip install boto3`) for interacting with AWS services.

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

### Run the Spark Jobs
Submit the Spark jobs using the `spark-submit` command:
```bash
spark-submit --master yarn --deploy-mode cluster spark_jobs/spark_job_1.py
spark-submit --master yarn --deploy-mode cluster spark_jobs/spark_job_2.py
```

### Explore Data Locally
Use the `notebooks/eda.ipynb` notebook to perform exploratory data analysis (EDA) and compute KPIs locally using PySpark. Open the notebook with Jupyter:
```bash
jupyter notebook notebooks/eda.ipynb
```

### Monitor Logs
Check the logs in CloudWatch or the local log file generated during execution:
```
car_rental_spark_job_<timestamp>.log
```

### Verify Output
The processed KPIs will be saved in the following S3 path:
```
s3://car-rental-bucket-125/processed_folder/output/user_transaction_kpis.parquet
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
├── data/                        # Local data folder (optional)
│   ├── raw/                     # Raw input data
│   └── processed/               # Processed output data
├── logs/                        # Log files generated during execution
├── requirements.txt             # Python dependencies
├── .env                         # Environment variables for AWS credentials
├── README.md                    # Project documentation
└── images/                      # Images used in the documentation
    └── architecture_diagram.jpg # Architecture diagram
```

---

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Submit a pull request with a detailed description of your changes.

---

## License

This project is licensed under the [MIT License](LICENSE). Feel free to use, modify, and distribute the code as needed.

---

## Contact

For questions or feedback, please contact:

- Email: your-email@example.com
- GitHub: [@your-github-username](https://github.com/your-github-username)

---

### Notes

- Replace placeholders like `your-repo`, `your-email@example.com`, and `@your-github-username` with actual values.
- If additional features or configurations are added to the project, update this `README.md` accordingly.
