from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, row_number
from pyspark.sql.window import Window


def main():
    spark = (
        SparkSession.builder
        .appName("DE1_PaySim_Quality_Check")
        .getOrCreate()
    )

    jdbc_url = "jdbc:postgresql://postgres:5432/bankdb"

    write_props = {
        "user": "etl",
        "password": "etl",
        "driver": "org.postgresql.Driver",
        "batchsize": "2000",
        "rewriteBatchedInserts": "true"
    }

    # Partitioned JDBC read with fetchsize to avoid OOM
    raw = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "raw_transactions")
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

    dup_cols = ["step", "type", "amount", "nameOrig", "nameDest"]
    window_spec = Window.partitionBy(*dup_cols).orderBy(lit(1))
    raw = raw.withColumn("rn", row_number().over(window_spec))

    bad_null_amount = col("amount").isNull()
    bad_amount_le0 = col("amount") <= 0
    bad_null_type = col("type").isNull()
    bad_null_nameOrig = col("nameOrig").isNull()
    bad_null_nameDest = col("nameDest").isNull()
    bad_dup = col("rn") > 1

    bad_any = (
        bad_null_amount |
        bad_amount_le0 |
        bad_null_type |
        bad_null_nameOrig |
        bad_null_nameDest |
        bad_dup
    )

    rejected = (
        raw.filter(bad_any)
        .withColumn(
            "reject_reason",
            when(bad_null_amount, lit("NULL_AMOUNT"))
            .when(bad_amount_le0, lit("AMOUNT_LE_ZERO"))
            .when(bad_null_type, lit("NULL_TYPE"))
            .when(bad_null_nameOrig, lit("NULL_NAMEORIG"))
            .when(bad_null_nameDest, lit("NULL_NAMEDEST"))
            .when(bad_dup, lit("DUPLICATE"))
            .otherwise(lit("UNKNOWN"))
        )
        .drop("rn")
    )

    clean = raw.filter(~bad_any).drop("rn")

    # Keep writes small for local Docker
    clean = clean.repartition(2)
    rejected = rejected.repartition(1)

    clean.write.jdbc(
        url=jdbc_url,
        table="transactions",
        mode="overwrite",
        properties=write_props
    )

    rejected.write.jdbc(
        url=jdbc_url,
        table="rejected_transactions",
        mode="overwrite",
        properties=write_props
    )

    print("Quality check complete")
    print("Clean rows:", clean.count())
    print("Rejected rows:", rejected.count())

    spark.stop()


if __name__ == "__main__":
    main()