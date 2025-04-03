from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
import logging
import sys
import traceback
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("job2_logger")

def create_spark_session():
    try:
        logger.info("Initializing Spark Session")
        spark = SparkSession.builder \
            .appName("Job1") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
        
        logger.info(f"Spark Version: {spark.version}")
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def load_data(spark, file_path, format_type="parquet"):
    try:
        logger.info(f"Loading data from {file_path}")
        if format_type == "parquet":
            df = spark.read.parquet(file_path)
        else:
            # Explicitly set CSV read parameters
            df = spark.read.csv(
                file_path, 
                header=True, 
                inferSchema=True
            )
        logger.info(f"Successfully loaded data from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to load data: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def compute_location_kpis(transactions):
    try:
        logger.info("Computing location-based KPIs")
        
        # Ensure timestamp columns are properly cast
        transactions = transactions.withColumn(
            "rental_start_time", 
            F.col("rental_start_time").cast(TimestampType())
        ).withColumn(
            "rental_end_time", 
            F.col("rental_end_time").cast(TimestampType())
        )

        revenue_per_location_df = transactions.groupBy("pickup_location").agg(F.sum("total_amount").alias("total_revenue"))
        transactions_per_location_df = transactions.groupBy("pickup_location").agg(F.count("rental_id").alias("total_transactions"))
        transaction_amounts_df = transactions.groupBy("pickup_location").agg(
            F.avg("total_amount").alias("avg_transaction"),
            F.max("total_amount").alias("max_transaction"),
            F.min("total_amount").alias("min_transaction")
        )
        unique_vehicles_per_location_df = transactions.groupBy("pickup_location").agg(F.countDistinct("vehicle_id").alias("unique_vehicles"))
        
        # Calculate rental duration with safely cast timestamps
        rental_duration_revenue_by_location_df = transactions.withColumn(
            "rental_duration_hours", 
            (F.unix_timestamp(F.col("rental_end_time")) - F.unix_timestamp(F.col("rental_start_time"))) / 3600
        ).groupBy("pickup_location").agg(
            F.sum("total_amount").alias("total_revenue_by_location"),
            F.sum("rental_duration_hours").alias("total_rental_duration_by_location")
        )

        final_kpi_df = revenue_per_location_df \
            .join(transactions_per_location_df, "pickup_location", "left") \
            .join(transaction_amounts_df, "pickup_location", "left") \
            .join(unique_vehicles_per_location_df, "pickup_location", "left") \
            .join(rental_duration_revenue_by_location_df, "pickup_location", "left")
        
        logger.info("Successfully computed location-based KPIs")
        return final_kpi_df
    except Exception as e:
        logger.error(f"Failed to compute KPIs: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def save_data(df, output_path, format_type="parquet"):
    try:
        logger.info(f"Saving data to {output_path}")
        if format_type == "parquet":
            df.write.mode("overwrite").parquet(output_path)
        else:
            df.write.mode("overwrite").csv(output_path, header=True)
        logger.info("Successfully saved data")
    except Exception as e:
        logger.error(f"Failed to save data: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def main():
    job_start_time = datetime.now()
    logger.info(f"Starting job at: {job_start_time}")
    
    try:
        transactions_s3_path = "s3://car-rental-bucket-125/land-folder/raw_data/rental_transactions.csv"
        output_s3_path = "s3://car-rental-bucket-125/processed_folder/output/location_kpis.parquet"
        
        spark = create_spark_session()
        # Explicitly set format_type to "csv" for the input file
        transactions = load_data(spark, transactions_s3_path, format_type="csv")
        location_kpis = compute_location_kpis(transactions)
        save_data(location_kpis, output_s3_path)
        
        job_end_time = datetime.now()
        logger.info(f"Job completed successfully at: {job_end_time}")
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()