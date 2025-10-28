-- create table northwind.territories
CREATE TABLE IF NOT EXISTS northwind."Territories"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_TERRITORIES_ID" SERIAL PRIMARY KEY,
    "TerritoryId" VARCHAR(20),
    "TerritoryDescription" VARCHAR(50) NOT NULL,
    "RegionId" INT NOT NULL
);