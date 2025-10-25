-- create table northwind.products

CREATE TABLE IF NOT EXISTS northwind.products
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(40) NOT NULL,
    supplier_id INT,
    category_id INT,
    quantity_per_unit VARCHAR(20),
    unit_price NUMERIC(10,4) DEFAULT 0.0000,
    units_in_stock SMALLINT DEFAULT 0,
    units_on_order SMALLINT DEFAULT 0,
    reorder_level SMALLINT DEFAULT 0,
    discontinued BOOLEAN NOT NULL DEFAULT FALSE
);