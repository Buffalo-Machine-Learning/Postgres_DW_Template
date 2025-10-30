"""
Build database by executing DDL scripts for all tables.
First, unique database, schemas, tables, stored procedures, and views are created.

Then, default database, schemas, tables, stored procedures, and views are created.
"""
# prevent pycache creation
import sys
sys.dont_write_bytecode = True

from utilities.postgres_wrapper import Postgres
import os

import atexit

def build_database():
    ddl_base_path = os.path.join(os.path.dirname(__file__), "DDL")
    
    # start with unique directory
    unique_path = os.path.join(ddl_base_path, "unique")

    db_database = Postgres(database_builder=True)

    # create the database first
    database_path = os.path.join(unique_path, "database")
    run_ddl(database_path, db_database)
    db_database.close()

    # now connect to the new database
    db = Postgres()
    atexit.register(db.close)

    # schemas
    schemas_path = os.path.join(unique_path, "schemas")
    run_ddl(schemas_path, db)
        
    # tables
    tables_path = os.path.join(unique_path, "tables")
    run_ddl(tables_path, db)

    # stored procedures
    sprocs_path = os.path.join(unique_path, "stored_procedures")
    run_ddl(sprocs_path, db)

    # views
    views_path = os.path.join(unique_path, "views")
    run_ddl(views_path, db)

    # default directory
    default_path = os.path.join(ddl_base_path, "default")

    # schemas
    schemas_path = os.path.join(default_path, "schemas")
    run_ddl(schemas_path, db)

    # tables
    tables_path = os.path.join(default_path, "tables")
    run_ddl(tables_path, db)

    # stored procedures
    sprocs_path = os.path.join(default_path, "stored_procedures")
    run_ddl(sprocs_path, db)

    # views
    views_path = os.path.join(default_path, "views")
    run_ddl(views_path, db)

    db.close()

def run_ddl(path: str, db: Postgres = None):
    if db is None:
        db = Postgres()
        atexit.register(db.close)

    for file in os.listdir(path):
        if file.endswith(".sql"):
            print(f"Running DDL: {file}")
            ddl_file = os.path.join(path, file)
            db.run_ddl(ddl_file)

if __name__ == "__main__":
    build_database()