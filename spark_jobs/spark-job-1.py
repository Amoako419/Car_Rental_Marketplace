from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import logging
import sys
import traceback
from datetime import datetime

# Set up logging with more detailed configuration
log_filename = f"car_rental_spark_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logger = logging.getLogger("car_rental_analytics")
logger.setLevel(logging.INFO)

# Create console handler with a higher log level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter and add it to the handlers
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(log_format)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(console_handler)


def main():
    logger.info("Starting Car Rental Analytics Spark Job")

    # Initialize Spark session with try-except
    try:
        logger.info("Initializing Spark Session")
        spark = SparkSession.builder \
            .appName("CarRentalAnalytics_Enhanced") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()

        # Enable Spark logging only for WARN level and above to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized successfully")
    except Exception as e:
        logger.critical(f"Failed to initialize Spark session: {str(e)}")
        logger.debug(traceback.format_exc())
        sys.exit(1)

    try:
        # Define S3 paths for input data
        s3_base_path = "s3://car-rental-bucket-125/land-folder/raw_data/"
        locations_s3_path = s3_base_path + "locations.csv"
        transactions_s3_path = s3_base_path + "rental_transactions.csv"
        users_s3_path = s3_base_path + "users.csv"
        vehicles_s3_path = s3_base_path + "vehicles.csv"

        # Define explicit schema definitions
        locations_schema = StructType([
            StructField("location_id", StringType(), True),
            StructField("location_name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True)
        ])

        transactions_schema = StructType([
            StructField("rental_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("vehicle_id", IntegerType(), True),
            StructField("rental_start_time", TimestampType(), True),
            StructField("rental_end_time", TimestampType(), True),
            StructField("pickup_location", StringType(), True),
            StructField("dropoff_location", StringType(), True),
            StructField("total_amount", FloatType(), True)
        ])

        users_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True)
        ])

        vehicles_schema = StructType([
            StructField("vehicle_id", IntegerType(), True),
            StructField("make", StringType(), True),
            StructField("model", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("vehicle_type", StringType(), True),
            StructField("license_plate", StringType(), True)
        ])

        # Load all datasets with appropriate error handling
        logger.info("Loading datasets from S3")
        locations = safe_read_csv(spark, locations_s3_path, locations_schema, "locations")
        transactions = safe_read_csv(spark, transactions_s3_path, transactions_schema, "transactions")
        users = safe_read_csv(spark, users_s3_path, users_schema, "users")
        vehicles = safe_read_csv(spark, vehicles_s3_path, vehicles_schema, "vehicles")

        # Validate required columns
        required_columns = {
            "transactions": ["rental_id", "pickup_location", "total_amount", "rental_start_time", "rental_end_time"],
            "locations": ["location_id", "location_name"],
            "users": ["user_id", "email"],
            "vehicles": ["vehicle_id", "vehicle_type"]
        }

        # Check for missing columns
        if not all([
            validate_columns(transactions, "transactions", required_columns["transactions"]),
            validate_columns(locations, "locations", required_columns["locations"]),
            validate_columns(users, "users", required_columns["users"]),
            validate_columns(vehicles, "vehicles", required_columns["vehicles"])
        ]):
            logger.error("Data validation failed. Exiting job.")
            spark.stop()
            sys.exit(1)

        # Data validation - check for nulls in key fields
        logger.info("Performing data quality checks")
        check_nulls_in_key_fields(transactions, "transactions", ["rental_id", "pickup_location", "total_amount"])

        # Ensure timestamps are in correct format
        logger.info("Processing timestamps")
        try:
            transactions = transactions.withColumn("rental_start_time", F.col("rental_start_time").cast("timestamp"))
            transactions = transactions.withColumn("rental_end_time", F.col("rental_end_time").cast("timestamp"))

            # Validate timestamp data
            invalid_timestamps = transactions.filter(
                F.col("rental_start_time").isNull() |
                F.col("rental_end_time").isNull() |
                (F.col("rental_end_time") < F.col("rental_start_time"))
            ).count()

            if invalid_timestamps > 0:
                logger.warning(f"Found {invalid_timestamps} records with invalid timestamps")
        except Exception as e:
            logger.error(f"Error processing timestamps: {str(e)}")
            logger.debug(traceback.format_exc())
            raise

        # Compute KPIs with robust error handling
        logger.info("Starting KPI calculations")
        kpi_dfs = calculate_kpis(transactions)

        # Join all KPIs on location
        logger.info("Joining KPI dataframes")
        try:
            final_kpi_df = join_kpi_dataframes(kpi_dfs)

            # Log schemas after each join
            logger.info(f"Final KPI DataFrame schema: {final_kpi_df.schema}")

            # Check for empty output
            if final_kpi_df.count() == 0:
                logger.warning("Warning: No KPIs generated. The resulting DataFrame is empty.")
            else:
                logger.info(f"Generated KPIs for {final_kpi_df.count()} locations")

            # S3 output path
            output_s3_path = "s3://car-rental-bucket-125/processed_folder/output/user_transaction_kpis.parquet"

            # Save the final KPIs as a single Parquet file to S3
            logger.info(f"Writing results to {output_s3_path}")
            try:
                final_kpi_df.coalesce(1).write.mode("overwrite").parquet(output_s3_path)
                logger.info(f"Successfully saved KPIs to {output_s3_path}")
            except Exception as e:
                logger.error(f"Failed to write output to S3: {str(e)}")
                logger.debug(traceback.format_exc())
                raise
        except Exception as e:
            logger.error(f"Error during KPI joining or writing: {str(e)}")
            logger.debug(traceback.format_exc())
            raise

        logger.info("Spark job completed successfully")
    except Exception as e:
        logger.error(f"Spark job failed: {str(e)}")
        logger.debug(traceback.format_exc())
        sys.exit(1)
    finally:
        # Always attempt to stop Spark session
        try:
            logger.info("Stopping Spark session")
            spark.stop()
        except Exception as e:
            logger.warning(f"Error stopping Spark session: {str(e)}")


# Function to read CSV with comprehensive error handling
def safe_read_csv(spark, path, schema, name):
    logger.info(f"Reading {name} data from {path}")
    try:
        df = spark.read.option("header", "true").schema(schema).csv(path)
        row_count = df.count()
        column_count = len(df.columns)
        logger.info(f"Successfully loaded {name} data: {row_count} rows, {column_count} columns")
        if row_count == 0:
            logger.warning(f"Warning: {name} dataset is empty")
        return df
    except Exception as e:
        logger.error(f"Error reading {name} data from {path}: {str(e)}")
        logger.debug(traceback.format_exc())
        raise


# Validate required columns
def validate_columns(df, df_name, required_cols):
    if df is None:
        logger.error(f"{df_name} DataFrame is missing")
        return False
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.error(f"{df_name} is missing columns: {missing_cols}")
        return False
    logger.info(f"{df_name} column validation passed")
    return True


# Check for nulls in key fields
def check_nulls_in_key_fields(df, df_name, key_fields):
    for field in key_fields:
        if field in df.columns:
            null_count = df.filter(F.col(field).isNull()).count()
            if null_count > 0:
                logger.warning(f"{df_name}.{field} contains {null_count} null values")
        else:
            logger.warning(f"Cannot check nulls for {field} as it doesn't exist in {df_name}")


# Calculate all KPIs with error handling
def calculate_kpis(transactions):
    kpi_dfs = {}

    # Revenue per Location
    try:
        logger.info("Calculating revenue per location")
        revenue_per_location_df = transactions.groupBy("pickup_location") \
            .agg(F.sum("total_amount").alias("total_revenue"))
        logger.info(f"Revenue per location schema: {revenue_per_location_df.schema}")
        kpi_dfs["revenue"] = revenue_per_location_df
    except Exception as e:
        logger.error(f"Error calculating revenue per location: {str(e)}")
        logger.debug(traceback.format_exc())
        raise

    # Total Transactions per Location
    try:
        logger.info("Calculating transactions per location")
        transactions_per_location_df = transactions.groupBy("pickup_location") \
            .agg(F.count("rental_id").alias("total_transactions"))
        logger.info(f"Transactions per location schema: {transactions_per_location_df.schema}")
        kpi_dfs["transactions"] = transactions_per_location_df
    except Exception as e:
        logger.error(f"Error calculating transactions per location: {str(e)}")
        logger.debug(traceback.format_exc())
        raise

    # Transaction amount statistics
    try:
        logger.info("Calculating transaction amount statistics")
        transaction_amounts_df = transactions.groupBy("pickup_location") \
            .agg(F.avg("total_amount").alias("avg_transaction"),
                 F.max("total_amount").alias("max_transaction"),
                 F.min("total_amount").alias("min_transaction"))
        logger.info(f"Transaction amounts schema: {transaction_amounts_df.schema}")
        kpi_dfs["amounts"] = transaction_amounts_df
    except Exception as e:
        logger.error(f"Error calculating transaction amount statistics: {str(e)}")
        logger.debug(traceback.format_exc())
        raise

    # Unique Vehicles Used at Each Location
    try:
        logger.info("Calculating unique vehicles per location")
        unique_vehicles_per_location_df = transactions.groupBy("pickup_location") \
            .agg(F.countDistinct("vehicle_id").alias("unique_vehicles"))
        logger.info(f"Unique vehicles schema: {unique_vehicles_per_location_df.schema}")
        kpi_dfs["vehicles"] = unique_vehicles_per_location_df
    except Exception as e:
        logger.error(f"Error calculating unique vehicles per location: {str(e)}")
        logger.debug(traceback.format_exc())
        raise

    # Rental Duration and Revenue by Location
    try:
        logger.info("Calculating rental duration metrics")
        rental_duration_revenue_by_location_df = transactions.withColumn(
            "rental_duration_hours",
            (F.unix_timestamp("rental_end_time") - F.unix_timestamp("rental_start_time")) / 3600
        ).groupBy("pickup_location") \
            .agg(F.sum("total_amount").alias("total_revenue_by_location"),
                 F.sum("rental_duration_hours").alias("total_rental_duration_by_location"),
                 F.avg("rental_duration_hours").alias("avg_rental_duration_by_location"))
        logger.info(f"Rental duration metrics schema: {rental_duration_revenue_by_location_df.schema}")
        kpi_dfs["duration"] = rental_duration_revenue_by_location_df
    except Exception as e:
        logger.error(f"Error calculating rental duration metrics: {str(e)}")
        logger.debug(traceback.format_exc())
        raise

    return kpi_dfs


# Join all KPI dataframes
def join_kpi_dataframes(kpi_dfs):
    try:
        # Start with revenue dataframe
        if "revenue" not in kpi_dfs:
            logger.error("Revenue DataFrame is missing. Cannot proceed with joining.")
            raise ValueError("Revenue DataFrame is missing.")

        final_df = kpi_dfs["revenue"]
        logger.info(f"Initial DataFrame (revenue) schema: {final_df.schema}")

        # Join all other KPI dataframes
        for key, df in kpi_dfs.items():
            if key != "revenue":
                logger.info(f"Joining DataFrame: {key}")
                logger.info(f"Schema of {key}: {df.schema}")
                final_df = final_df.join(df, "pickup_location", "left")
                logger.info(f"Joined {key}. New schema: {final_df.schema}")

        return final_df
    except Exception as e:
        logger.error(f"Error joining KPI dataframes: {str(e)}")
        logger.debug(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()