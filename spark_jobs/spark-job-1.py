from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Job2").getOrCreate()
# S3 paths for the input data
locations_s3_path = "s3://car-rental-bucket-125/land-folder/raw_data/locations.csv"
transactions_s3_path = "s3://car-rental-bucket-125/land-folder/raw_data/rental_transactions.csv"
users_s3_path = "s3://car-rental-bucket-125/land-folder/raw_data/users.csv"
vehicles_s3_path = "s3://car-rental-bucket-125/land-folder/raw_data/vehicles.csv"

# Load data from S3
locations = spark.read.csv(locations_s3_path, header=True, inferSchema=True)
transactions = spark.read.csv(transactions_s3_path, header=True, inferSchema=True)
users = spark.read.csv(users_s3_path, header=True, inferSchema=True)
vehicles = spark.read.csv(vehicles_s3_path, header=True, inferSchema=True)

# Revenue per Location
revenue_per_location_df = transactions.groupBy("pickup_location").agg(F.sum("total_amount").alias("total_revenue"))

# Total Transactions per Location
transactions_per_location_df = transactions.groupBy("pickup_location").agg(F.count("rental_id").alias("total_transactions"))

# Average, Max, and Min Transaction Amounts
transaction_amounts_df = transactions.groupBy("pickup_location").agg(
    F.avg("total_amount").alias("avg_transaction"),
    F.max("total_amount").alias("max_transaction"),
    F.min("total_amount").alias("min_transaction")
)


# Unique Vehicles Used at Each Location
unique_vehicles_per_location_df = transactions.groupBy("pickup_location").agg(F.countDistinct("vehicle_id").alias("unique_vehicles"))

# Rental Duration and Revenue by Location
# Calculate rental duration in hours and aggregate revenue and duration by location
rental_duration_revenue_by_location_df = transactions.withColumn(
    "rental_duration_hours", 
    (F.col("rental_end_time").cast("long") - F.col("rental_start_time").cast("long")) / 3600
).groupBy("pickup_location").agg(
    F.sum("total_amount").alias("total_revenue_by_location"),
    F.sum("rental_duration_hours").alias("total_rental_duration_by_location")
)

# Join all KPIs on location (and vehicle_type where applicable)

final_kpi_df = revenue_per_location_df \
        .join(transactions_per_location_df, "pickup_location", "left") \
        .join(transaction_amounts_df, "pickup_location", "left") \
        .join(unique_vehicles_per_location_df, "pickup_location", "left") \
        .join(rental_duration_revenue_by_location_df, "pickup_location", "left")


# S3 output path
output_s3_path = "s3://car-rental-bucket-125/processed_folder/output/user_transaction_kpis.parquet"

# Save the final KPIs as a single Parquet file to S3
final_kpi_df.write.mode("overwrite").parquet(output_s3_path)