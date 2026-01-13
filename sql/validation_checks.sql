-- Duplicate transaction check
SELECT transaction_id, COUNT(*)
FROM financial_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Null amount check
SELECT COUNT(*)
FROM financial_transactions
WHERE amount IS NULL;

-- High-value transaction monitoring
SELECT *
FROM financial_transactions
WHERE amount > 10000;
