-- create table northwind.customer_demographics
CREATE TABLE IF NOT EXISTS northwind.customer_demographics
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "dw_customer_demographics_id" SERIAL PRIMARY KEY,
    CustomerTypeId VARCHAR(10) NOT NULL,
    CustomerDesc TEXT
);