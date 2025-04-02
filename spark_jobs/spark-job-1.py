from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys

def main():
    spark = SparkSession.builder \
        .appName("Job2") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    try:
        # 1. Validate Input Paths
        input_bucket = "s3://car-rental-bucket-125/land-folder/raw_data/"
        required_files = ["locations.csv", "rental_transactions.csv", "users.csv", "vehicles.csv"]
        for file in required_files:
            if not spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            ).exists(spark._jvm.org.apache.hadoop.fs.Path(input_bucket + file)):
                raise FileNotFoundError(f"Missing input file: {file}")

        # 2. Load Data with Explicit Schemas
        transactions_schema = StructType([
            StructField("rental_id", StringType()),
            StructField("vehicle_id", StringType()),
            StructField("pickup_location", StringType()),
            StructField("total_amount", DoubleType()),
            StructField("rental_start_time", TimestampType()),
            StructField("rental_end_time", TimestampType())
        ])

        transactions = spark.read.csv(
            input_bucket + "rental_transactions.csv",
            schema=transactions_schema,
            header=True
        ).na.fill({"total_amount": 0, "pickup_location": "UNKNOWN"})

        # 3. Calculate KPIs
        kpi_dfs = [
            transactions.groupBy("pickup_location")
                .agg(F.sum("total_amount").alias("total_revenue")),
                
            transactions.groupBy("pickup_location")
                .agg(F.count("rental_id").alias("total_transactions")),
                
            transactions.groupBy("pickup_location").agg(
                F.avg("total_amount").alias("avg_transaction"),
                F.max("total_amount").alias("max_transaction"),
                F.min("total_amount").alias("min_transaction")
            ),
            
            transactions.withColumn(
                "rental_duration_hours",
                (F.col("rental_end_time").cast("long") - F.col("rental_start_time").cast("long")) / 3600
            ).groupBy("pickup_location").agg(
                F.sum("total_amount").alias("total_revenue_by_location"),
                F.sum("rental_duration_hours").alias("total_rental_duration_by_location")
            )
        ]

        # 4. Join all KPIs
        final_kpi = kpi_dfs[0]
        for df in kpi_dfs[1:]:
            final_kpi = final_kpi.join(df, "pickup_location", "left")

        # 5. Write Output
        output_path = "s3://car-rental-bucket-125/processed_folder/output/user_transaction_kpis/"
        final_kpi.repartition(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)

    except Exception as e:
        print(f"Job failed: {str(e)}", file=sys.stderr)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()