-- create table northwind.territories
CREATE TABLE IF NOT EXISTS northwind.territories
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "dw_territories_id" SERIAL PRIMARY KEY,
    TerritoryId VARCHAR(20) PRIMARY KEY,
    TerritoryDescription VARCHAR(50) NOT NULL,
    RegionId INT NOT NULL
);