-- create table northwind.region

CREATE TABLE IF NOT EXISTS northwind."Regions"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_REGIONS_ID" SERIAL PRIMARY KEY,
    "RegionID" INT,
    "RegionDescription" VARCHAR(50) NOT NULL
);