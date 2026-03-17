import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():

    spark = SparkSession.builder \
        .appName("DE1_PaySim_Quality_Check") \
        .getOrCreate()

    print("✅ Quality Check started")

    # ✅ FIXED DB CONNECTION
    url = "jdbc:postgresql://host.docker.internal:5432/bankdb"

    properties = {
        "user": "etl",
        "password": "etl",
        "driver": "org.postgresql.Driver"
    }

    # ✅ Read final table
    df = spark.read \
        .jdbc(
            url=url,
            table="final_transactions",
            properties=properties
        )

    print(f"Total rows: {df.count()}")

    # ✅ Example checks
    null_count = df.filter(col("amount").isNull()).count()
    negative_count = df.filter(col("amount") < 0).count()

    print(f"Null amounts: {null_count}")
    print(f"Negative amounts: {negative_count}")

    if null_count > 0 or negative_count > 0:
        raise Exception("❌ Data Quality Failed")
    else:
        print("✅ Data Quality Passed")

    spark.stop()


if __name__ == "__main__":
    main()