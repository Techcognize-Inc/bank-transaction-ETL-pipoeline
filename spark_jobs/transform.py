import argparse
from pyspark.sql import SparkSession


def main(staging_path: str):
    spark = (
        SparkSession.builder
        .appName("DE1_PaySim_Transform")
        .getOrCreate()
    )

    jdbc_url = "jdbc:postgresql://postgres:5432/bankdb"

    # Read in JDBC partitions, similar to quality_check fix
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "transactions")
        .option("user", "etl")
        .option("password", "etl")
        .option("driver", "org.postgresql.Driver")
        .option("partitionColumn", "step")
        .option("lowerBound", 1)
        .option("upperBound", 743)
        .option("numPartitions", 4)
        .option("fetchsize", 1000)
        .load()
    )

    # Avoid unnecessary large shuffle / pressure
    (
        df.repartition(2, "type")
          .write
          .mode("overwrite")
          .partitionBy("type")
          .parquet(staging_path)
    )

    print(f"Parquet written to: {staging_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging", required=True, help="Output parquet path inside container")
    args = parser.parse_args()
    main(args.staging)