-- create table northwind.customers

CREATE TABLE IF NOT EXISTS northwind."Customers"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_CUSTOMERS_ID" SERIAL PRIMARY KEY,
    "CustomerID" VARCHAR(5) NOT NULL,
    "CompanyName" VARCHAR(40) NOT NULL,
    "ContactName" VARCHAR(30),
    "ContactTitle" VARCHAR(30),
    "Address" VARCHAR(60),
    "City" VARCHAR(15),
    "Region" VARCHAR(15),
    "PostalCode" VARCHAR(10),
    "Country" VARCHAR(15),
    "Phone" VARCHAR(24),
    "Fax" VARCHAR(24)
);

CREATE TRIGGER trg_touch_date_modified_customers
BEFORE UPDATE ON northwind."Customers"
FOR EACH ROW
EXECUTE FUNCTION common.touch_date_modified();