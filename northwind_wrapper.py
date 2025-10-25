import requests
import pandas as pd

class Northwind:
    def __init__(self):
        self.base_url = "https://services.odata.org/V4/Northwind/Northwind.svc/"
        self.session = requests.Session()

    def query(self, entity: str, **kwargs) -> pd.DataFrame:
        url = f"{self.base_url}{entity}"
        response = self.session.get(url, params=kwargs)
        response.raise_for_status()
        data = response.json()
        return pd.json_normalize(data["value"])