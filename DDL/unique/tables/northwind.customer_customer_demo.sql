-- create table northwind.customer_customer_demo

CREATE TABLE IF NOT EXISTS northwind.customer_customer_demo
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "dw_customer_customer_demo_id" SERIAL PRIMARY KEY,
    CustomerId VARCHAR(5) NOT NULL,
    CustomerTypeId VARCHAR(10) NOT NULL
);