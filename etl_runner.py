"""
Built to follow SSIS conventions
"""
import sys
sys.dont_write_bytecode = True # Prevent .pyc files

import pandas as pd
from postgres_wrapper import Postgres
import atexit

db = Postgres()
atexit.register(db.close)

out = db.query_simple(schema="common", table="IngestionLog", limit=5)

print(type(out))
print(out.describe())
print(out.head())