import pandas as pd

# prevent pycache creation
import sys
sys.dont_write_bytecode = True

from postgres_wrapper import Postgres
from northwind_wrapper import Northwind
from etl_runner import ETLRunner

if __name__ == "__main__":
    runner = ETLRunner(Northwind, Postgres)
    
    runner.truncate_reload(
        schema="northwind",
        table_name="Customers",
        source_query="SELECT * FROM Customers",
        batch_size=5000
    )

    runner.insert_latest(
        schema="northwind",
        table_name="Orders",
        source_query="SELECT * FROM Orders",
        max_field="OrderID",
        batch_size=5000
    )

    runner.insert_latest(
        schema="northwind",
        table_name="Categories",
        source_query="SELECT * FROM Categories",
        batch_size=5000
    )

    runner.insert_latest(
        schema="northwind",
        table_name="Products",
        source_query="SELECT * FROM Products",
        max_field="ProductID",
        batch_size=5000
    )
