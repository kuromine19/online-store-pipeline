-- Warehouse schema for the online store demo. Safe to run repeatedly.
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS audit;

-- Staging: cleaned rows for the current run only (truncated each run)
CREATE TABLE IF NOT EXISTS stg.dim_category (category_id text, category_name text);
CREATE TABLE IF NOT EXISTS stg.dim_payment_method (payment_method_code text, payment_method_name text);
CREATE TABLE IF NOT EXISTS stg.dim_customer (customer_id text, full_name text, email text, city text, signup_date date);
CREATE TABLE IF NOT EXISTS stg.dim_product (product_id text, product_name text, category_id text, unit_price numeric(12,2));
CREATE TABLE IF NOT EXISTS stg.fact_orders (order_id text, order_ts timestamp, customer_id text, payment_method_code text, status text);
CREATE TABLE IF NOT EXISTS stg.fact_order_items (order_id text, line_no int, product_id text, quantity int, unit_price numeric(12,2));

-- Every staged row is tagged with the business date of the run that staged it, so runs for
-- different days never read each other's rows (safe for backfills and overlapping runs).
ALTER TABLE stg.dim_category ADD COLUMN IF NOT EXISTS business_date date;
ALTER TABLE stg.dim_payment_method ADD COLUMN IF NOT EXISTS business_date date;
ALTER TABLE stg.dim_customer ADD COLUMN IF NOT EXISTS business_date date;
ALTER TABLE stg.dim_product ADD COLUMN IF NOT EXISTS business_date date;
ALTER TABLE stg.fact_orders ADD COLUMN IF NOT EXISTS business_date date;
ALTER TABLE stg.fact_order_items ADD COLUMN IF NOT EXISTS business_date date;

-- Core: conformed dimensions and facts (upserted)
CREATE TABLE IF NOT EXISTS core.dim_category (
    category_id text PRIMARY KEY, category_name text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS core.dim_payment_method (
    payment_method_code text PRIMARY KEY, payment_method_name text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS core.dim_customer (
    customer_id text PRIMARY KEY, full_name text, email text, city text, signup_date date,
    updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS core.dim_product (
    product_id text PRIMARY KEY, product_name text NOT NULL, category_id text NOT NULL REFERENCES core.dim_category,
    unit_price numeric(12,2) NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS core.fact_orders (
    order_id text PRIMARY KEY, order_date date NOT NULL, order_ts timestamp NOT NULL,
    customer_id text NOT NULL REFERENCES core.dim_customer,
    payment_method_code text NOT NULL REFERENCES core.dim_payment_method,
    status text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS core.fact_order_items (
    order_id text NOT NULL, line_no int NOT NULL, product_id text NOT NULL REFERENCES core.dim_product,
    quantity int NOT NULL, unit_price numeric(12,2) NOT NULL, line_amount numeric(14,2) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (order_id, line_no));

-- Marts: reporting tables rebuilt each run
CREATE TABLE IF NOT EXISTS mart.daily_sales (
    sales_date date PRIMARY KEY, orders int, items int, revenue numeric(14,2), avg_order_value numeric(12,2));
CREATE TABLE IF NOT EXISTS mart.category_sales (
    sales_date date, category_name text, revenue numeric(14,2), items int, PRIMARY KEY (sales_date, category_name));

-- Audit: what happened in every run
CREATE TABLE IF NOT EXISTS audit.rejected_rows (
    id bigserial PRIMARY KEY, run_date date NOT NULL, table_name text NOT NULL, reason text NOT NULL,
    payload jsonb NOT NULL, rejected_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS audit.load_stats (
    run_date date NOT NULL, table_name text NOT NULL, extracted int, rejected int, loaded int,
    loaded_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (run_date, table_name));
CREATE TABLE IF NOT EXISTS audit.quality_results (
    run_date date NOT NULL, check_name text NOT NULL, severity text NOT NULL, failed_rows int NOT NULL,
    passed boolean NOT NULL, checked_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (run_date, check_name));
