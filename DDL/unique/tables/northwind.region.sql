-- create table northwind.region

CREATE TABLE IF NOT EXISTS northwind.region
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    region_id INT PRIMARY KEY,
    region_description VARCHAR(50) NOT NULL
);