-- create table northwind.order_details

CREATE TABLE IF NOT EXISTS northwind.order_details
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "dw_order_details_id" SERIAL PRIMARY KEY,
    OrderId INT NOT NULL,
    ProductId INT NOT NULL,
    UnitPrice NUMERIC(10,4) NOT NULL,
    Quantity INT NOT NULL,
    Discount REAL NOT NULL,
);