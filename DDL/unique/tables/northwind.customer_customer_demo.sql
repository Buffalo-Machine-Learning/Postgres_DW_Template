-- create table northwind."Customer_Customer_Demo"
CREATE TABLE IF NOT EXISTS northwind."Customer_Customer_Demo"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_CUSTOMER_CUSTOMER_DEMO_ID" SERIAL PRIMARY KEY,
    "CustomerId" VARCHAR(5) NOT NULL,
    "CustomerTypeId" VARCHAR(10) NOT NULL
);