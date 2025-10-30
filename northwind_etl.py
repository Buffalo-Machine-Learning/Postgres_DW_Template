import pandas as pd

# prevent pycache creation
import sys
sys.dont_write_bytecode = True

from utilities.postgres_wrapper import Postgres
from northwind_wrapper import Northwind
from utilities.etl_runner import ETLRunner

if __name__ == "__main__":
    runner = ETLRunner(Northwind, Postgres)
    
    runner.insert_latest(
        schema="northwind",
        table_name="Categories",
        source_query="SELECT * FROM Categories",
        max_field="CategoryID",
        batch_size=5000
    )

    runner.insert_latest(
        schema="northwind",
        table_name="CustomerDemographics",
        source_query="SELECT * FROM CustomerDemographics",
        batch_size=5000
    )

    runner.truncate_reload(
        schema="northwind",
        table_name="Customers",
        source_query="SELECT * FROM Customers",
        batch_size=5000
    )

    runner.insert_latest(
        schema="northwind",
        table_name="Employees",
        source_query="SELECT * FROM Employees",
        max_field="EmployeeID",
        batch_size=5000
    )

    runner.insert_latest(
        schema="northwind",
        table_name="Order_Details",
        source_query="SELECT * FROM Order_Details",
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
        table_name="Products",
        source_query="SELECT * FROM Products",
        max_field="ProductID",
        batch_size=5000
    )

    runner.insert_latest(
        schema="northwind",
        table_name="Regions",
        source_query="SELECT * FROM Regions",
        max_field="RegionID",
        batch_size=5000
    )

    runner.insert_latest(
        schema="northwind",
        table_name="Shippers",
        source_query="SELECT * FROM Shippers",
        max_field="ShipperID",
        batch_size=5000
    )

    runner.insert_latest(
        schema="northwind",
        table_name="Suppliers",
        source_query="SELECT * FROM Suppliers",
        max_field="SupplierID",
        batch_size=5000
    )

    runner.truncate_reload(
        schema="northwind",
        table_name="Territories",
        source_query="SELECT * FROM Territories",
        batch_size=5000
    )