from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgresConnectionTest") \
    .getOrCreate()

data = [
    (1, "PAYMENT", 1000.50),
    (2, "TRANSFER", 2500.75)
]

df = spark.createDataFrame(data, ["id", "transaction_type", "amount"])

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/bank_etl") \
    .option("dbtable", "test_transactions") \
    .option("user", "etl_user") \
    .option("password", "etl_password") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Data written successfully to PostgreSQL")

spark.stop()
