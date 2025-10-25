-- create table northwind.customers

CREATE TABLE IF NOT EXISTS northwind.customers
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    customer_id VARCHAR(5) NOT NULL PRIMARY KEY,
    company_name VARCHAR(40) NOT NULL,
    contact_name VARCHAR(30),
    contact_title VARCHAR(30),
    address VARCHAR(60),
    city VARCHAR(15),
    region VARCHAR(15),
    postal_code VARCHAR(10),
    country VARCHAR(15),
    phone VARCHAR(24),
    fax VARCHAR(24)
);