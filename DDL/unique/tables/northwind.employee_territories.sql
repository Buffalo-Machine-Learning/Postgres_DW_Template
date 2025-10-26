-- create table northwind.employee_territories
CREATE TABLE IF NOT EXISTS northwind.employee_territories
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "dw_employee_territories_id" SERIAL PRIMARY KEY,
    EmployeeId INT NOT NULL,
    TerritoryId VARCHAR(20) NOT NULL
);