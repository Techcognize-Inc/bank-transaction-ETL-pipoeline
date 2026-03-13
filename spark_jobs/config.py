JDBC_URL = "jdbc:postgresql://postgres:5432/bankdb"
DB_PROPERTIES = {
    "user": "etl",
    "password": "etl",
    "driver": "org.postgresql.Driver",
    "batchsize": "2000",
    "rewriteBatchedInserts": "true"
}

RAW_TABLE = "raw_transactions"
CLEAN_TABLE = "transactions"
REJECTED_TABLE = "rejected_transactions"
SUMMARY_TABLE = "daily_account_summary"
STAGING_PATH = "/opt/staging/transactions_parquet"