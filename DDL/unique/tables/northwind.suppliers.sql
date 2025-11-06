-- create table northwind.suppliers
CREATE TABLE IF NOT EXISTS northwind."Suppliers"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_SUPPLIERS_ID" SERIAL PRIMARY KEY,
    "SupplierID" INT,
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

CREATE TRIGGER trg_touch_date_modified_suppliers
BEFORE UPDATE ON northwind."Suppliers"
FOR EACH ROW
EXECUTE FUNCTION common.touch_date_modified();