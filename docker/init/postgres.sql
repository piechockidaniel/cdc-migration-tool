ALTER ROLE cdc WITH REPLICATION;
CREATE TABLE public.customers (customer_id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT, city TEXT);
ALTER TABLE public.customers REPLICA IDENTITY FULL;
CREATE TABLE public.customers_target (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT, city TEXT);
