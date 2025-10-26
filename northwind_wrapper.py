import requests
import pandas as pd

class Northwind:
    def __init__(self):
        self.base_url = "https://services.odata.org/V4/Northwind/Northwind.svc/"
        self.session = requests.Session()
    
    def query_table(self, table_name: str) -> pd.DataFrame:
        url = f"{self.base_url}{table_name}"
        response = self.session.get(url)
        response.raise_for_status()
        data = response.json().get('value', [])
        return pd.DataFrame(data)

    