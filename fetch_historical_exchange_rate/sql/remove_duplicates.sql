-- Remove duplicate rows from fact_exchange_rate (SQLite)
DELETE FROM fact_exchange_rate
WHERE rowid NOT IN (
  SELECT MIN(rowid)
  FROM fact_exchange_rate
  GROUP BY exchange_date, base_currency, target_currency
);