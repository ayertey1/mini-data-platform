CREATE TABLE IF NOT EXISTS sales (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    product_name TEXT,
    quantity INTEGER,
    unit_price NUMERIC(12,2),
    total_amount NUMERIC(12,2),
    created_at TIMESTAMP DEFAULT now()
);
