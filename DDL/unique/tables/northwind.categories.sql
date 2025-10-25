-- create table northwind.categories

CREATE TABLE IF NOT EXISTS northwind.categories
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(15) NOT NULL,
    description TEXT,
    picture BYTEA
);