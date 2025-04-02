from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys

def main():
    # Initialize Spark with performance optimizations
    spark = SparkSession.builder \
        .appName("Job1") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    try:
        # 1. Input Validation
        input_bucket = "s3://car-rental-bucket-125/land-folder/raw_data/"
        required_files = {
            "locations.csv": StructType([
                StructField("location_id", StringType()),
                StructField("city", StringType()),
                StructField("state", StringType())
            ]),
            "rental_transactions.csv": StructType([
                StructField("rental_id", StringType()),
                StructField("user_id", StringType()),
                StructField("vehicle_id", StringType()),
                StructField("pickup_location", StringType()),
                StructField("total_amount", DoubleType()),
                StructField("rental_start_time", TimestampType()),
                StructField("rental_end_time", TimestampType())
            ]),
            "users.csv": StructType([
                StructField("user_id", StringType()),
                StructField("name", StringType()),
                StructField("membership_level", StringType())
            ]),
            "vehicles.csv": StructType([
                StructField("vehicle_id", StringType()),
                StructField("make", StringType()),
                StructField("model", StringType()),
                StructField("vehicle_type", StringType())
            ])
        }

        # Check file existence and load with schemas
        data = {}
        for file, schema in required_files.items():
            path = input_bucket + file
            if not spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            ).exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
                raise FileNotFoundError(f"Missing input file: {file}")
            
            data[file.split('.')[0]] = spark.read.csv(
                path,
                schema=schema,
                header=True
            ).na.fill({
                "total_amount": 0,
                "pickup_location": "UNKNOWN",
                "user_id": "UNKNOWN_USER"
            })

        transactions = data["rental_transactions"]
        
        # 2. Data Quality Checks
        if transactions.count() == 0:
            raise ValueError("No transactions found in the input data")
            
        # 3. KPI Calculations with Null Handling
        transactions = transactions.withColumn(
            "rental_date", 
            F.date_format(F.col("rental_start_time"), "yyyy-MM-dd")
        ).filter(
            F.col("rental_start_time").isNotNull() & 
            F.col("rental_end_time").isNotNull()
        )

        # Daily Metrics
        transactions_per_day = transactions.groupBy("rental_date") \
            .agg(F.count("rental_id").alias("total_transactions"))
            
        revenue_per_day = transactions.groupBy("rental_date") \
            .agg(F.sum("total_amount").alias("total_revenue"))

        # User Metrics
        user_metrics = transactions.groupBy("user_id") \
            .agg(
                F.sum("total_amount").alias("total_spent"),
                (F.avg(
                    F.col("rental_end_time").cast("long") - 
                    F.col("rental_start_time").cast("long")
                )/3600).alias("avg_rental_duration_hours")
            )

        # Transaction Extremes
        transaction_amounts = transactions.agg(
            F.max("total_amount").alias("max_transaction_amount"),
            F.min("total_amount").alias("min_transaction_amount")
        ).crossJoin(
            transactions.groupBy().agg(F.count("rental_id").alias("total_transactions_all_time"))
        )
        
        # 4. Join KPIs
        daily_kpis = transactions_per_day.join(
            revenue_per_day, 
            "rental_date", 
            "inner"
        )
        
        # 5. Output Configuration
        output_path = "s3://car-rental-bucket-125/processed_folder/output/daily_kpis/"
        
        # Write with optimal partitioning
        daily_kpis.repartition(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path + "daily/")
            
        user_metrics.repartition(1) \
            .write \
            .mode("overwrite") \
            .parquet(output_path + "user_metrics/")
            
        transaction_amounts.write \
            .mode("overwrite") \
            .json(output_path + "transaction_extremes/")

    except Exception as e:
        print(f"Job failed: {str(e)}", file=sys.stderr)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()