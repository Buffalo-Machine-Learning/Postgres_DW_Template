import requests
import pandas as pd
import re

BASE_URL = "https://services.odata.org/V4/Northwind/Northwind.svc"
class Northwind:
    def __init__(self):
        self.session = requests.Session()

    def close(self):
        self.session.close()

    def query(self, query: str) -> pd.DataFrame:
        fields = re.search(r"SELECT (.+) FROM", query, re.IGNORECASE)
        table = re.search(r"FROM (\w+)", query, re.IGNORECASE)
        where = re.search(r"WHERE (.+)", query, re.IGNORECASE)

        if not (fields and table):
            raise ValueError("Invalid query")

        resp = self.session.get(
            f"{BASE_URL}/{table.group(1)}",
            params={
                "$select": fields.group(1),
                "$filter": where.group(1) if where else None,
                "$format": "json"
            }
        )

        df = pd.json_normalize(resp.json()["value"])
        return df