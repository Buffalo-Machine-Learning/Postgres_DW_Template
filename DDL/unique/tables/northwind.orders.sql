-- create table northwind.orders

CREATE TABLE IF NOT EXISTS northwind.orders
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    order_id INT NOT NULL PRIMARY KEY,
    customer_id VARCHAR(5) NOT NULL,
    employee_id INT NOT NULL,
    order_date DATE NOT NULL,
    required_date DATE NOT NULL,
    shipped_date DATE,
    ship_via INT NOT NULL,
    freight NUMERIC(10,2) NOT NULL,
    ship_name VARCHAR(40) NOT NULL,
    ship_address VARCHAR(60) NOT NULL,
    ship_city VARCHAR(15) NOT NULL,
    ship_region VARCHAR(15),
    ship_postal_code VARCHAR(10),
    ship_country VARCHAR(15) NOT NULL
);
