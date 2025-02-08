from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    expr, to_date, col, countDistinct, sum as _sum, lit
)
import sys
import time


def initialize_spark_session(master_url):
    """Initialize and return a Spark session."""
    return SparkSession.builder \
        .master(master_url) \
        .appName("Generate_Report") \
        .getOrCreate()


def read_data(spark, local_path, s3_path):
    """Read data from a local path or S3."""
    try:
        print("Attempting to read local file...")
        return spark.read.csv(local_path, header=True, inferSchema=True)
    except Exception as e:
        print(f"Local file not found. Falling back to S3. Error: {e}")
        return spark.read.csv(s3_path, header=True, inferSchema=True)


def process_data(df):
    """Process the input DataFrame by cleaning, transforming, and aggregating data."""
    df = df.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "MM/dd/yyyy HH:mm")) \
           .withColumn("UnitPrice", col("UnitPrice").cast("float")) \
           .withColumn("Quantity", col("Quantity").cast("int")) \
           .withColumn("CustomerID", col("CustomerID").cast("int")) \
           .fillna({"CustomerID": 99999}) \
           .filter((col("Quantity") > 0) & (col("UnitPrice") > 0)) \
           .dropDuplicates() \
           .withColumn("InvoiceMonthYear", expr("date_format(InvoiceDate, 'yyyy-MM')")) \
           .withColumn("TotalSales", col("Quantity") * col("UnitPrice"))

    return df


def generate_summary(df):
    """Generate a monthly summary from the processed DataFrame."""
    total_unique_customers = df.select("CustomerID").distinct().count()

    monthly_summary = df.groupBy("InvoiceMonthYear").agg(
        countDistinct("StockCode").alias("total_products_sold"),
        _sum("Quantity").alias("total_quantity"),
        _sum("TotalSales").alias("total_sales"),
        countDistinct("CustomerID").alias("customers_bought")
    ).withColumn(
        "customers_who_bought_nothing", lit(total_unique_customers) - col("customers_bought")
    ).orderBy(col("InvoiceMonthYear").desc())

    return monthly_summary


def save_summary(monthly_summary, output_path):
    """Save the summary DataFrame to the specified S3 path."""
    timestamp = time.strftime("%Y-%m-%d-%H-%M-%S")
    output_dir = f"{output_path}/spark_output/report-{timestamp}"
    monthly_summary.coalesce(1).write.csv(output_dir, header=True)
    print(f"Report saved to: {output_dir}")


def generate_report(local_csv_path, s3_input_path, s3_output_path, spark_master_url):
    """Main function to generate the report."""
    spark = initialize_spark_session(spark_master_url)

    print("Reading data...")
    df = read_data(spark, local_csv_path, s3_input_path)
    df.show()

    print("Processing data...")
    processed_df = process_data(df)

    print("Generating summary...")
    monthly_summary = generate_summary(processed_df)
    monthly_summary.show()

    print("Saving summary to S3...")
    save_summary(monthly_summary, s3_output_path)

    print("Stopping Spark session...")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: generate_report.py <LOCAL_CSV_PATH> <S3_INPUT_PATH> <S3_OUTPUT_PATH> <SPARK_MASTER_URL>")
        sys.exit(-1)

    local_csv_path = sys.argv[1]
    s3_input_path = sys.argv[2]
    s3_output_path = sys.argv[3]
    spark_master_url = sys.argv[4]

    generate_report(local_csv_path, s3_input_path, s3_output_path, spark_master_url)
