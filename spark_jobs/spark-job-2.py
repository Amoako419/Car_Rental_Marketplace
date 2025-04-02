from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

logger = logging.getLogger("job1_logger")

def create_spark_session():
    """Create and return a Spark session with appropriate configuration."""
    try:
        logger.info("Initializing Spark Session")
        spark = SparkSession.builder \
            .appName("Job1") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
        
        # Log Spark configuration for debugging
        logger.info(f"Spark Version: {spark.version}")
        logger.info(f"Application ID: {spark.sparkContext.applicationId}")
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def load_data(spark, file_path, format_type="csv"):
    """Load data from S3 with error handling."""
    try:
        logger.info(f"Loading data from {file_path}")
        if format_type == "csv":
            df = spark.read.csv(file_path, header=True, inferSchema=True)
        elif format_type == "parquet":
            df = spark.read.parquet(file_path)
        else:
            raise ValueError(f"Unsupported format type: {format_type}")
        
        logger.info(f"Successfully loaded data from {file_path}")
        logger.info(f"Schema: {df.schema}")
        logger.info(f"Record count: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Failed to load data from {file_path}: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def process_transactions(transactions_df):
    """Process transaction data with error handling."""
    try:
        logger.info("Processing transactions data")
        
        # Convert rental_start_time to date for daily aggregations
        transactions_with_date = transactions_df.withColumn(
            "rental_date", 
            F.date_format(F.col("rental_start_time"), "yyyy-MM-dd")
        )
        
        logger.info("Successfully processed transactions data")
        return transactions_with_date
    except Exception as e:
        logger.error(f"Failed to process transactions: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def compute_daily_metrics(transactions_df):
    """Compute daily transaction metrics with error handling."""
    try:
        logger.info("Computing daily transaction metrics")
        
        # Compute Total Transactions per Day
        transactions_per_day = transactions_df.groupBy("rental_date").agg(
            F.count("rental_id").alias("total_transactions")
        )
        logger.info(f"Transactions per day count: {transactions_per_day.count()}")
        
        # Compute Revenue per Day
        revenue_per_day = transactions_df.groupBy("rental_date").agg(
            F.sum("total_amount").alias("total_revenue")
        )
        logger.info(f"Revenue per day count: {revenue_per_day.count()}")
        
        # Compute Transaction Amounts
        transaction_amounts = transactions_df.agg(
            F.max("total_amount").alias("max_transaction_amount"),
            F.min("total_amount").alias("min_transaction_amount")
        )
        
        logger.info("Successfully computed daily metrics")
        return transactions_per_day, revenue_per_day, transaction_amounts
    except Exception as e:
        logger.error(f"Failed to compute daily metrics: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def compute_user_metrics(transactions_df):
    """Compute user-specific metrics with error handling."""
    try:
        logger.info("Computing user metrics")
        
        # Compute User-specific Spending and Rental Duration Metrics
        user_metrics = transactions_df.groupBy("user_id").agg(
            F.sum("total_amount").alias("total_spent"),
            F.avg(
                F.col("rental_end_time").cast("long") - 
                F.col("rental_start_time").cast("long")
            ).alias("avg_rental_duration")
        )
        
        logger.info(f"User metrics count: {user_metrics.count()}")
        logger.info("Successfully computed user metrics")
        return user_metrics
    except Exception as e:
        logger.error(f"Failed to compute user metrics: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def join_metrics(transactions_per_day, revenue_per_day, transaction_amounts):
    """Join metrics dataframes with error handling."""
    try:
        logger.info("Joining metrics dataframes")
        
        kpi_df = transactions_per_day \
            .join(revenue_per_day, "rental_date", "inner") \
            .join(transaction_amounts)
        
        logger.info(f"Final KPI dataframe count: {kpi_df.count()}")
        logger.info("Successfully joined metrics dataframes")
        return kpi_df
    except Exception as e:
        logger.error(f"Failed to join metrics dataframes: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def save_data(df, output_path, format_type="parquet"):
    """Save dataframe to S3 with error handling."""
    try:
        logger.info(f"Saving data to {output_path}")
        
        if format_type == "parquet":
            df.write.mode("overwrite").parquet(output_path)
        elif format_type == "csv":
            df.write.mode("overwrite").csv(output_path)
        else:
            raise ValueError(f"Unsupported format type: {format_type}")
        
        logger.info(f"Successfully saved data to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save data to {output_path}: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def main():
    """Main function to orchestrate the entire job."""
    job_start_time = datetime.now()
    logger.info(f"Starting job at: {job_start_time}")
    
    try:
        # S3 paths for the input data
        locations_s3_path = "s3://car-rental-bucket-125/land-folder/raw_data/locations.csv"
        transactions_s3_path = "s3://car-rental-bucket-125/land-folder/raw_data/rental_transactions.csv"
        users_s3_path = "s3://car-rental-bucket-125/land-folder/raw_data/users.csv"
        vehicles_s3_path = "s3://car-rental-bucket-125/land-folder/raw_data/vehicles.csv"
        
        # Output path
        output_s3_path = "s3://car-rental-bucket-125/processed_folder/output/user_transaction_kpis.parquet"
        
        # Create Spark session
        spark = create_spark_session()
        
        # Load data from S3
        logger.info("Loading datasets from S3")
        try:
            locations = load_data(spark, locations_s3_path)
            transactions = load_data(spark, transactions_s3_path)
            users = load_data(spark, users_s3_path)
            vehicles = load_data(spark, vehicles_s3_path)
        except Exception as e:
            logger.error("Failed to load one or more datasets")
            raise
        
        # Process transactions data
        processed_transactions = process_transactions(transactions)
        
        # Compute metrics
        transactions_per_day, revenue_per_day, transaction_amounts = compute_daily_metrics(processed_transactions)
        user_metrics = compute_user_metrics(processed_transactions)
        
        # Join metrics to create final KPI dataframe
        kpi_df = join_metrics(transactions_per_day, revenue_per_day, transaction_amounts)
        
        # Save the final KPIs as a Parquet file to S3
        save_data(kpi_df, output_s3_path)
        
        # Also save user metrics as a separate file
        user_metrics_output = "s3://car-rental-bucket-125/processed_folder/output/user_metrics.parquet"
        save_data(user_metrics, user_metrics_output)
        
        job_end_time = datetime.now()
        execution_time = (job_end_time - job_start_time).total_seconds()
        logger.info(f"Job completed successfully at: {job_end_time}")
        logger.info(f"Total execution time: {execution_time} seconds")
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        logger.error(traceback.format_exc())
        # Exit with error code
        sys.exit(1)

if __name__ == "__main__":
    main()