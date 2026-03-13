from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count as _count, current_date


def main():
    spark = (
        SparkSession.builder
        .appName("DE1_PaySim_Aggregate")
        .getOrCreate()
    )

    jdbc_url = "jdbc:postgresql://postgres:5432/bankdb"

    props = {
        "user": "etl",
        "password": "etl",
        "driver": "org.postgresql.Driver",
        "batchsize": "2000",
        "rewriteBatchedInserts": "true"
    }

    # Read clean transactions in smaller JDBC partitions
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

    summary = (
        df.withColumn("load_date", current_date())
          .groupBy("load_date", "nameOrig", "type")
          .agg(
              _sum("amount").alias("total_amount"),
              _count("*").alias("txn_count")
          )
          .repartition(2)
    )

    summary.write.jdbc(
        url=jdbc_url,
        table="daily_account_summary",
        mode="overwrite",
        properties=props
    )

    print("Aggregation complete")
    print("Summary rows:", summary.count())

    spark.stop()


if __name__ == "__main__":
    main()