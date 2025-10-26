"""
Build database by executing DDL scripts for all tables.
First, unique database, schemas, tables, stored procedures, and views are created.

Then, default database, schemas, tables, stored procedures, and views are created.
"""

from postgres_wrapper import Postgres
import os

import atexit

db = Postgres()
atexit.register(db.close)

def build_database():
    ddl_base_path = os.path.join(os.path.dirname(__file__), "DDL")
    
    # start with unique directory
    unique_path = os.path.join(ddl_base_path, "unique")

    # schemas
    schemas_path = os.path.join(unique_path, "schemas")
    run_ddl(schemas_path)
        
    # tables
    tables_path = os.path.join(unique_path, "tables")
    run_ddl(tables_path)

    # stored procedures
    sprocs_path = os.path.join(unique_path, "stored_procedures")
    run_ddl(sprocs_path)

    # views
    views_path = os.path.join(unique_path, "views")
    run_ddl(views_path)

    # default directory
    default_path = os.path.join(ddl_base_path, "default")

    # schemas
    schemas_path = os.path.join(default_path, "schemas")
    run_ddl(schemas_path)

    # tables
    tables_path = os.path.join(default_path, "tables")
    run_ddl(tables_path)

    # stored procedures
    sprocs_path = os.path.join(default_path, "stored_procedures")
    run_ddl(sprocs_path)

    # views
    views_path = os.path.join(default_path, "views")
    run_ddl(views_path)

    db.close()

def run_ddl(path: str):
    for file in os.listdir(path):
        if file.endswith(".sql"):
            print(f"Running DDL: {file}")
            ddl_file = os.path.join(path, file)
            db.run_ddl(ddl_file)

if __name__ == "__main__":
    build_database()