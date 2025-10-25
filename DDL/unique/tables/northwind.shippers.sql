-- create table northwind.shippers
CREATE TABLE IF NOT EXISTS northwind.shippers
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    shipper_id INT PRIMARY KEY,
    company_name VARCHAR(40) NOT NULL,
    phone VARCHAR(24)
);