CREATE TABLE IF NOT EXISTS raw_transactions (
  step INT,
  type TEXT,
  amount NUMERIC(18,2),
  nameOrig TEXT,
  oldbalanceOrg NUMERIC(18,2),
  newbalanceOrig NUMERIC(18,2),
  nameDest TEXT,
  oldbalanceDest NUMERIC(18,2),
  newbalanceDest NUMERIC(18,2),
  isFraud INT,
  isFlaggedFraud INT
);

CREATE TABLE IF NOT EXISTS transactions (
  step INT,
  type TEXT,
  amount NUMERIC(18,2),
  nameDest TEXT,
  oldbalanceDest NUMERIC(18,2),
  newbalanceDest NUMERIC(18,2),
  isFraud INT,
  isFlaggedFraud INT,
  load_date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS rejected_transactions (
  step INT,
  type TEXT,
  amount NUMERIC(18,2),
  nameOrig TEXT,
  oldbalanceOrg NUMERIC(18,2),
  newbalanceOrig NUMERIC(18,2),
  nameDest TEXT,
  oldbalanceDest NUMERIC(18,2),
  newbalanceDest NUMERIC(18,2),
  isFraud INT,
  isFlaggedFraud INT,
  reject_reason TEXT,
  rejected_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_account_summary (
  load_date DATE,
  nameOrig TEXT,
  type TEXT,
  total_amount NUMERIC(18,2),
  txn_count BIGINT,
  PRIMARY KEY (load_date, nameOrig, type)
);
