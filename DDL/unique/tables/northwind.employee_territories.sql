-- create table northwind.employee_territories
CREATE TABLE IF NOT EXISTS northwind."Employee_Territories"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_EMPLOYEE_TERRITORIES_ID" SERIAL PRIMARY KEY,
    "EmployeeId" INT NOT NULL,
    "TerritoryId" VARCHAR(20) NOT NULL
);