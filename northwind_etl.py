import pandas as pd

# prevent pycache creation
import sys
sys.dont_write_bytecode = True

from postgres_wrapper import Postgres
from northwind_wrapper import Northwind
from etl_runner import ETLRunner

if __name__ == "__main__":
    runner = ETLRunner(Northwind, Postgres)
    
    runner.insert_latest(
        schema="northwind",
        table_name="customers",
        source_query="SELECT * FROM Customers",
        max_field="customerid",
        batch_size=5000
    )