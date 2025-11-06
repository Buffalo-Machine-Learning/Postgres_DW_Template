-- create table northwind.territories
CREATE TABLE IF NOT EXISTS northwind."Territories"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_TERRITORIES_ID" SERIAL PRIMARY KEY,
    "TerritoryID" VARCHAR(20),
    "TerritoryDescription" VARCHAR(50) NOT NULL
);

CREATE TRIGGER trg_touch_date_modified_territories
BEFORE UPDATE ON northwind."Territories"
FOR EACH ROW
EXECUTE FUNCTION common.touch_date_modified();