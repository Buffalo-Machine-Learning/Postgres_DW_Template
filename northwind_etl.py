import pandas as pd
from postgres_wrapper import Postgres
from northwind_wrapper import Northwind
from etl_runner import ETLRunner

if __name__ == "__main__":
    runner = ETLRunner(Northwind, Postgres)
    
    runner.insert_latest(
        schema="northwind",
        table_name="customers",
        source_query="SELECT * FROM Customers",
        max_field="customer_id",
        batch_size=5000
    )