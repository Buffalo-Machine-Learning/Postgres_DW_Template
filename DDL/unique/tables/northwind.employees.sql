-- create table northwind.employees
CREATE TABLE IF NOT EXISTS northwind."Employees"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_EMPLOYEES_ID" SERIAL PRIMARY KEY,
    "EmployeeID" int,
    "LastName" VARCHAR(20) NOT NULL,
    "FirstName" VARCHAR(10) NOT NULL,
    "Title" VARCHAR(30),
    "TitleOfCourtesy" VARCHAR(25),
    "BirthDate" DATE,
    "HireDate" DATE,
    "Address" VARCHAR(60),
    "City" VARCHAR(15),
    "Region" VARCHAR(15),
    "PostalCode" VARCHAR(10),
    "Country" VARCHAR(15),
    "HomePhone" VARCHAR(24),
    "Extension" VARCHAR(4),
    "Photo" BYTEA,
    "Notes" TEXT,
    "ReportsTo" FLOAT,
    "PhotoPath" VARCHAR(255)
);

CREATE TRIGGER trg_touch_date_modified_employees
BEFORE UPDATE ON northwind."Employees"
FOR EACH ROW
EXECUTE FUNCTION common.touch_date_modified();