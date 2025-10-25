-- create table northwind.customer_demographics
CREATE TABLE IF NOT EXISTS northwind.customer_demographics
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    customer_type_id VARCHAR(10) NOT NULL PRIMARY KEY,
    customer_desc TEXT
);