-- Runs once, on the first start of the postgres container.
CREATE USER warehouse WITH PASSWORD 'warehouse';
CREATE DATABASE warehouse OWNER warehouse;
