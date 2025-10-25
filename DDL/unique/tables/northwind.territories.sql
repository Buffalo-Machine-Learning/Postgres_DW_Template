-- create table northwind.territories
CREATE TABLE IF NOT EXISTS northwind.territories
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    territory_id VARCHAR(20) PRIMARY KEY,
    territory_description VARCHAR(50) NOT NULL,
    region_id INT NOT NULL,
    CONSTRAINT fk_territories_region FOREIGN KEY (region_id) REFERENCES northwind.region (region_id)
);