-- create table northwind.region

CREATE TABLE IF NOT EXISTS northwind.region
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "dw_region_id" SERIAL PRIMARY KEY,
    RegionId INT,
    RegionDescription VARCHAR(50) NOT NULL
);