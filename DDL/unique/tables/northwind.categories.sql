-- create table northwind.categories

CREATE TABLE IF NOT EXISTS northwind."Categories"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_CATEGORIES_ID" SERIAL PRIMARY KEY,
    "CategoryId" BIGINT,
    "CategoryName" VARCHAR(15) NOT NULL,
    "Description" TEXT,
    "Picture" BYTEA
);