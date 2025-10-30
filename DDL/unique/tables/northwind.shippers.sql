-- create table northwind.shippers
CREATE TABLE IF NOT EXISTS northwind."Shippers"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_SHIPPERS_ID" SERIAL PRIMARY KEY,
    "ShipperID" INT,
    "CompanyName" VARCHAR(40) NOT NULL,
    "Phone" VARCHAR(24)
);