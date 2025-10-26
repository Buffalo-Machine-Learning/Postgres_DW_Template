-- create table northwind.us_states

CREATE TABLE IF NOT EXISTS northwind.us_states
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    "dw_us_states_id" SERIAL PRIMARY KEY,
    StateCode CHAR(2) NOT NULL,
    StateName VARCHAR(50) NOT NULL
);