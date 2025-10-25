-- create table northwind.employees
CREATE TABLE IF NOT EXISTS northwind.employees
(
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
    employee_id SERIAL PRIMARY KEY,
    last_name VARCHAR(20) NOT NULL,
    first_name VARCHAR(10) NOT NULL,
    title VARCHAR(30),
    title_of_courtesy VARCHAR(25),
    birth_date DATE,
    hire_date DATE,
    address VARCHAR(60),
    city VARCHAR(15),
    region VARCHAR(15),
    postal_code VARCHAR(10),
    country VARCHAR(15),
    home_phone VARCHAR(24),
    extension VARCHAR(4),
    photo BYTEA,
    notes TEXT,
    reports_to INT,
    photo_path VARCHAR(255)
);