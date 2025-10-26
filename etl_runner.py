import pandas as pd
import atexit

class ETLRunner:
    def __init__(self, source_class, dest_class):
        self.source = source_class()
        self.dest = dest_class()
        atexit.register(self.source.close)
        atexit.register(self.dest.close)

    def insert_latest(self,
        schema: str,
        table_name: str,
        source_query: str,
        max_field: str | None = None,
        batch_size: int = 10000):
        
        # extract data from souce, optionally filtering for latest records
        if max_field:
            max_value = self.dest.get_max_value(schema, table_name, max_field)
            if max_value is not None:
                source_query += f" WHERE {max_field} > '{max_value}'"

        data = self.source.query(source_query)

        print(data.describe(include='all'))
        print(data.head())

        # load data into destination
        self.dest.insert_data(
            schema, 
            table_name,
            data,
            source=self.source.__class__.__name__,
            batch_size=batch_size
        )

        print(f"Inserted {len(data)} records into {schema}.{table_name} from {self.source.__class__.__name__}")
        
