import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)

PAY_SIM_SCHEMA = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
])

def main(input_path: str):
    spark = (
        SparkSession.builder
        .appName("DE1_PaySim_Ingest_CSV")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", "true")
        .schema(PAY_SIM_SCHEMA)
        .csv(input_path)
    )

    print("✅ Ingest started")
    print("Input:", input_path)
    print("Rows:", df.count())
    df.printSchema()

    jdbc_url = "jdbc:postgresql://postgres:5432/bankdb"
    props = {"user": "etl", "password": "etl", "driver": "org.postgresql.Driver"}

    (
        df.write
        .mode("overwrite")
        .jdbc(url=jdbc_url, table="raw_transactions", properties=props)
    )

    print("✅ Written to Postgres: raw_transactions")
    spark.stop()
    print("✅ Done")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    main(args.input)
