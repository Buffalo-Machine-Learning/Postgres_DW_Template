import requests
import pandas as pd
import re

from utilities.odata_base_wrapper import OData

BASE_URL = "https://services.odata.org/V4/Northwind/Northwind.svc"

class Northwind(OData):
    def __init__(self):
        super().__init__(url=BASE_URL)
        