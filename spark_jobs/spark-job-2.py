from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Job1").getOrCreate()
# S3 paths for the input data
locations_s3_path = "s3://your-bucket-name/prefix/locations.csv"
transactions_s3_path = "s3://your-bucket-name/prefix/rental_transactions.csv"
users_s3_path = "s3://your-bucket-name/prefix/users.csv"
vehicles_s3_path = "s3://your-bucket-name/prefix/vehicles.csv"

# Load data from S3
locations = spark.read.csv(locations_s3_path, header=True, inferSchema=True)
transactions = spark.read.csv(transactions_s3_path, header=True, inferSchema=True)
users = spark.read.csv(users_s3_path, header=True, inferSchema=True)
vehicles = spark.read.csv(vehicles_s3_path, header=True, inferSchema=True)

# Convert rental_start_time to date for daily aggregations
transactions = transactions.withColumn("rental_date", F.date_format(F.col("rental_start_time"), "yyyy-MM-dd"))

# Compute Total Transactions per Day
transactions_per_day = transactions.groupBy("rental_date").agg(F.count("rental_id").alias("total_transactions"))

# Compute Revenue per Day
revenue_per_day = transactions.groupBy("rental_date").agg(F.sum("total_amount").alias("total_revenue"))


# Compute User-specific Spending and Rental Duration Metrics
user_metrics = transactions.groupBy("user_id").agg(
    F.sum("total_amount").alias("total_spent"),
    F.avg(F.col("rental_end_time").cast("long") - F.col("rental_start_time").cast("long")).alias("avg_rental_duration")
)


# Compute Maximum and Minimum Transaction Amounts
transaction_amounts = transactions.agg(
    F.max("total_amount").alias("max_transaction_amount"),
    F.min("total_amount").alias("min_transaction_amount")
)

kpi_df = transactions_per_day \
    .join(revenue_per_day, "rental_date", "inner") \
    .join(transaction_amounts)


# S3 output path
output_s3_path = "s3://your-bucket-name/prefix/output/user_transaction_kpis.parquet"

# Save the final KPIs as a single Parquet file to S3
kpi_df.write.mode("overwrite").parquet(output_s3_path)