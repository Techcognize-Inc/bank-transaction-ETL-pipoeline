import argparse
from pyspark.sql import SparkSession


def main(input_path):

    # ✅ Create Spark Session
    spark = SparkSession.builder \
        .appName("DE1_PaySim_Ingest_CSV") \
        .getOrCreate()

    print("✅ Ingest started")
    print(f"Input: {input_path}")

    # ✅ Read CSV
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(input_path)

    # ✅ Debug info (important for Jenkins logs)
    print(f"Rows: {df.count()}")
    df.printSchema()

    # ✅ PostgreSQL connection (FIXED)
    url = "jdbc:postgresql://host.docker.internal:5432/bankdb"

    properties = {
        "user": "etl",
        "password": "etl",
        "driver": "org.postgresql.Driver"
    }

    # ✅ Write to PostgreSQL
    df.write \
        .jdbc(
            url=url,
            table="raw_transactions",
            mode="overwrite",
            properties=properties
        )

    print("✅ Data successfully written to PostgreSQL")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to input CSV file")

    args = parser.parse_args()

    main(args.input)