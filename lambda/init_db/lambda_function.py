import json
import boto3
import psycopg2
import os
import logging
import time
from datetime import datetime
from aws_xray_sdk.core import patch_all

patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client('secretsmanager')
_db_credentials = None

def get_db_credentials():
    global _db_credentials
    if _db_credentials:
        return _db_credentials
    secret_arn = os.environ['SECRET_ARN']
    response = secrets_client.get_secret_value(SecretId=secret_arn)
    _db_credentials = json.loads(response['SecretString'])
    return _db_credentials


def get_db_connection():
    creds = get_db_credentials()
    for attempt in range(3):
        try:
            return psycopg2.connect(
                host=creds['host'],
                database=creds['dbname'],
                user=creds['username'],
                password=creds['password'],
                connect_timeout=10
            )
        except Exception as e:
            if attempt == 2:
                raise e
            time.sleep(2 ** attempt)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INT NOT NULL DEFAULT 0,
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES customers(customer_id),
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    total_amount DECIMAL(10,2) NOT NULL,
    payment_transaction_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(30) REFERENCES orders(order_id),
    product_id VARCHAR(20) REFERENCES products(product_id),
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE customers ADD COLUMN IF NOT EXISTS address TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_transaction_id VARCHAR(50);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_customers_updated_at ON customers;
CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS trg_products_updated_at ON products;
CREATE TRIGGER trg_products_updated_at
BEFORE UPDATE ON products
FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS trg_orders_updated_at ON orders;
CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION update_updated_at();
"""

SAMPLE_CUSTOMERS = [
    ("CUST001", "Alice Johnson", "alice@example.com", "+1-555-0101"),
    ("CUST002", "Bob Smith", "bob@example.com", "+1-555-0102"),
    ("CUST003", "Carol White", "carol@example.com", "+1-555-0103"),
    ("CUST004", "David Brown", "david@example.com", "+1-555-0104"),
    ("CUST005", "Eva Martinez", "eva@example.com", "+1-555-0105"),
]

SAMPLE_PRODUCTS = [
    ("PROD001", "Laptop Pro 15", "Electronics", 1299.99, 25),
    ("PROD002", "Wireless Mouse", "Accessories", 29.99, 150),
    ("PROD003", "USB-C Hub 7-in-1", "Accessories", 49.99, 80),
    ("PROD004", "4K Monitor 27\"", "Electronics", 449.99, 15),
    ("PROD005", "Mechanical Keyboard", "Accessories", 89.99, 60),
    ("PROD006", "NVMe SSD 1TB", "Storage", 119.99, 200),
    ("PROD007", "Webcam 4K", "Electronics", 79.99, 45),
    ("PROD008", "Laptop Stand", "Accessories", 34.99, 3),  # Low stock
    ("PROD009", "External HDD 2TB", "Storage", 89.99, 90),
    ("PROD010", "Smart Speaker", "Electronics", 149.99, 2),  # Low stock
]

SAMPLE_ORDERS = [
    ("ORD-2026010001", "CUST001", "completed", 1329.98),
    ("ORD-2026010002", "CUST002", "processing", 49.99),
    ("ORD-2026010003", "CUST003", "pending", 449.99),
    ("ORD-2026010004", "CUST004", "failed", 29.99),
    ("ORD-2026010005", "CUST005", "completed", 209.98),
]


def lambda_handler(event, context):
    logger.info(json.dumps({'action': 'init_db_start', 'event': event}))

    insert_sample = event.get('insert_sample_data', True)
    drop_existing = event.get('drop_existing', False)

    conn = get_db_connection()
    results = []

    try:
        with conn.cursor() as cur:
            if drop_existing:
                logger.warning("Dropping existing tables!")
                cur.execute("DROP TABLE IF EXISTS order_items, orders, products, customers CASCADE")
                results.append("Dropped existing tables")

            # Execute schema
            cur.execute(SCHEMA_SQL)
            results.append("Schema created/updated successfully")
            conn.commit()

            if insert_sample:
                # Insert customers
                for c in SAMPLE_CUSTOMERS:
                    cur.execute("""
                        INSERT INTO customers (customer_id, name, email, phone)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (customer_id) DO NOTHING
                    """, c)

                # Insert products
                for p in SAMPLE_PRODUCTS:
                    cur.execute("""
                        INSERT INTO products (product_id, name, category, price, stock_quantity)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (product_id) DO NOTHING
                    """, p)

                # Insert orders
                for o in SAMPLE_ORDERS:
                    cur.execute("""
                        INSERT INTO orders (order_id, customer_id, status, total_amount)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (order_id) DO NOTHING
                    """, o)

                # Insert order items for first order
                cur.execute("""
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                    VALUES ('ORD-2026010001', 'PROD001', 1, 1299.99),
                           ('ORD-2026010001', 'PROD002', 1, 29.99)
                    ON CONFLICT DO NOTHING
                """)

                conn.commit()
                results.append(f"Inserted {len(SAMPLE_CUSTOMERS)} customers, {len(SAMPLE_PRODUCTS)} products, {len(SAMPLE_ORDERS)} orders")

        logger.info(json.dumps({'action': 'init_db_complete', 'results': results}))
        return {'statusCode': 200, 'results': results}

    except Exception as e:
        conn.rollback()
        logger.error(f"DB init failed: {e}")
        raise e
    finally:
        conn.close()
