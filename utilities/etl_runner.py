import pandas as pd
import atexit

class ETLRunner:
    def __init__(self, source_class, dest_class):
        self.source = source_class()
        self.source_name = source_class.__name__
        self.dest = dest_class()
        atexit.register(self.source.close)
        atexit.register(self.dest.close)

    def truncate_reload(self, 
        schema: str, 
        table_name: str, 
        source_query: str, 
        batch_size: int = 10000):

        # truncate destination table
        self.dest.truncate_table(schema, table_name, self.source_name)

        # run insert
        self.insert_latest(
            schema=schema,
            table_name=table_name,
            source_query=source_query,
            batch_size=batch_size
        )

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
                if isinstance(max_value, int) or isinstance(max_value, float):
                    source_query += f" WHERE {max_field} > {max_value}"
                else:
                    source_query += f" WHERE {max_field} > '{max_value}'"

        print(f"Extracting data from source with query: {source_query}")

        data = self.source.query(source_query)

        # load data into destination
        self.dest.insert_data(
            schema, 
            table_name,
            data,
            source=self.source_name,
            batch_size=batch_size
        )

        print(f"Inserted {len(data)} records into {schema}.{table_name} from {self.source_name}.\n")

