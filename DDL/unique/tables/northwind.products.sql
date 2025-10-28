-- create table northwind.products

CREATE TABLE IF NOT EXISTS northwind."Products"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_PRODUCTS_ID" SERIAL PRIMARY KEY,
    "ProductName" VARCHAR(40) NOT NULL,
    "SupplierId" INT,
    "CategoryId" INT,
    "QuantityPerUnit" VARCHAR(20),
    "UnitPrice" NUMERIC(10,4) DEFAULT 0.0000,
    "UnitsInStock" SMALLINT DEFAULT 0,
    "UnitsOnOrder" SMALLINT DEFAULT 0,
    "ReorderLevel" SMALLINT DEFAULT 0,
    "Discontinued" BOOLEAN NOT NULL DEFAULT FALSE
);