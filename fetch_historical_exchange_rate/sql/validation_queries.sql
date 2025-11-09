-- Check most recent dates
SELECT DISTINCT exchange_date FROM fact_exchange_rate ORDER BY exchange_date DESC LIMIT 5;

-- Average AUD rate over past week
SELECT AVG(rate) AS avg_aud_rate
FROM fact_exchange_rate
WHERE target_currency = 'AUD';

-- Day-to-day change for USD
SELECT
    exchange_date,
    rate - LAG(rate) OVER (ORDER BY exchange_date) AS rate_change
FROM fact_exchange_rate
WHERE target_currency = 'USD';
