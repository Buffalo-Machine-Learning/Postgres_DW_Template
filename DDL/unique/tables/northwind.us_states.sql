-- create table northwind.us_states

CREATE TABLE IF NOT EXISTS northwind."US_States"
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "DW_US_STATES_ID" SERIAL PRIMARY KEY,
    "StateCode" CHAR(2) NOT NULL,
    "StateName" VARCHAR(50) NOT NULL
);