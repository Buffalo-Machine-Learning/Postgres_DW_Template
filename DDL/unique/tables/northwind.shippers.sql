-- create table northwind.shippers
CREATE TABLE IF NOT EXISTS northwind.shippers
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "dw_shipper_id" SERIAL PRIMARY KEY,
    ShipperId INT PRIMARY KEY,
    CompanyName VARCHAR(40) NOT NULL,
    Phone VARCHAR(24)
);