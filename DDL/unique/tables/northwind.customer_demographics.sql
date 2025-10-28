-- create table northwind.customer_demographics
CREATE TABLE IF NOT EXISTS northwind."Customer_Demographics"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_CUSTOMER_DEMOGRAPHICS_ID" SERIAL PRIMARY KEY,
    "CustomerTypeId" VARCHAR(10) NOT NULL,
    "CustomerDesc" TEXT
);