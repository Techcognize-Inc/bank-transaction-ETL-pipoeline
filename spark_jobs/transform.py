import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main(staging_table):

    # ✅ Create Spark Session
    spark = SparkSession.builder \
        .appName("DE1_PaySim_Transform") \
        .getOrCreate()

    print("✅ Transform started")
    print(f"Reading from table: {staging_table}")

    # ✅ PostgreSQL connection (FIXED)
    url = "jdbc:postgresql://host.docker.internal:5432/bankdb"

    properties = {
        "user": "etl",
        "password": "etl",
        "driver": "org.postgresql.Driver"
    }

    # ✅ Read data from PostgreSQL
    df = spark.read \
        .jdbc(
            url=url,
            table=staging_table,
            properties=properties
        )

    print(f"Rows read: {df.count()}")
    df.printSchema()

    # ✅ Simple transformation (example)
    transformed_df = df.withColumn("amount", col("amount") * 1.0)

    print("✅ Transformation applied")

    # ✅ Write transformed data back to PostgreSQL
    transformed_df.write \
        .jdbc(
            url=url,
            table="final_transactions",
            mode="overwrite",
            properties=properties
        )

    print("✅ Data successfully written to final_transactions")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging", required=True, help="Staging table name")

    args = parser.parse_args()

    main(args.staging)