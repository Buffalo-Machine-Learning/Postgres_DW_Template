-- create table northwind.order_details

CREATE TABLE IF NOT EXISTS northwind."Order_Details"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_ORDER_DETAILS_ID" SERIAL PRIMARY KEY,
    "OrderID" INT NOT NULL,
    "ProductID" INT NOT NULL,
    "UnitPrice" NUMERIC(10,4) NOT NULL,
    "Quantity" INT NOT NULL,
    "Discount" REAL NOT NULL
);