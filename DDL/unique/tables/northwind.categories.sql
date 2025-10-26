-- create table northwind.categories

CREATE TABLE IF NOT EXISTS northwind.categories
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "dw_category_id" SERIAL PRIMARY KEY,
    CategoryId BIGINT,
    CategoryName VARCHAR(15) NOT NULL,
    Description TEXT,
    Picture BYTEA
);