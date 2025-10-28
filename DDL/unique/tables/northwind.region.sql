-- create table northwind.region

CREATE TABLE IF NOT EXISTS northwind."Region"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_REGION_ID" SERIAL PRIMARY KEY,
    "RegionId" INT,
    "RegionDescription" VARCHAR(50) NOT NULL
);