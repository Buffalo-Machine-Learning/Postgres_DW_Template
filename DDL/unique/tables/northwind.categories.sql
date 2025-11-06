-- create table northwind.categories

CREATE TABLE IF NOT EXISTS northwind."Categories"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_CATEGORIES_ID" SERIAL PRIMARY KEY,
    "CategoryID" BIGINT,
    "CategoryName" VARCHAR(15) NOT NULL,
    "Description" TEXT,
    "Picture" BYTEA
);

CREATE TRIGGER trg_touch_date_modified_categories
BEFORE UPDATE ON northwind."Categories"
FOR EACH ROW
EXECUTE FUNCTION common.touch_date_modified();