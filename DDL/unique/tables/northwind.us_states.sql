-- create table northwind.us_states

CREATE TABLE IF NOT EXISTS northwind.us_states
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    state_code CHAR(2) PRIMARY KEY,
    state_name VARCHAR(50) NOT NULL
);