-- create table northwind.customer_demographics
CREATE TABLE IF NOT EXISTS northwind."CustomerDemographics"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_CUSTOMERDEMOGRAPHICS_ID" SERIAL PRIMARY KEY,
    "CustomerTypeID" VARCHAR(10) NOT NULL,
    "CustomerDesc" TEXT
);
CREATE TRIGGER trg_touch_date_modified_cust_demo
BEFORE UPDATE ON northwind."CustomerDemographics"
FOR EACH ROW
EXECUTE FUNCTION common.touch_date_modified();
