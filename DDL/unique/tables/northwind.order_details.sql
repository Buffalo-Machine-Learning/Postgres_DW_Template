-- create table northwind.order_details

CREATE TABLE IF NOT EXISTS northwind.order_details
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    unit_price NUMERIC(10,4) NOT NULL,
    quantity INT NOT NULL,
    discount REAL NOT NULL,
    CONSTRAINT pk_order_details PRIMARY KEY (order_id, product_id)
);