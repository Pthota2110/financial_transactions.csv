CREATE TABLE financial_transactions (
    transaction_id VARCHAR(50),
    transaction_date DATE,
    account_id VARCHAR(50),
    customer_id VARCHAR(50),
    transaction_type VARCHAR(10),
    amount DECIMAL(12,2),
    currency VARCHAR(5),
    country VARCHAR(50),
    merchant VARCHAR(100),
    amount_category VARCHAR(20)
);
