-- create table northwind.customer_customer_demo

CREATE TABLE IF NOT EXISTS northwind.customer_customer_demo
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    customer_id VARCHAR(5) NOT NULL,
    customer_type_id VARCHAR(10) NOT NULL,
    CONSTRAINT pk_customer_customer_demo PRIMARY KEY (customer_id, customer_type_id)
);