-- create table northwind.orders

CREATE TABLE IF NOT EXISTS northwind."Orders"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_ORDERS_ID" SERIAL PRIMARY KEY,
    "OrderID" INT NOT NULL,
    "CustomerID" VARCHAR(5) NOT NULL,
    "EmployeeID" INT NOT NULL,
    "OrderDate" DATE NOT NULL,
    "RequiredDate" DATE NOT NULL,
    "ShippedDate" DATE,
    "ShipVia" INT NOT NULL,
    "Freight" NUMERIC(10,2) NOT NULL,
    "ShipName" VARCHAR(40) NOT NULL,
    "ShipAddress" VARCHAR(60) NOT NULL,
    "ShipCity" VARCHAR(15) NOT NULL,
    "ShipRegion" VARCHAR(15),
    "ShipPostalCode" VARCHAR(10),
    "ShipCountry" VARCHAR(15) NOT NULL
);
