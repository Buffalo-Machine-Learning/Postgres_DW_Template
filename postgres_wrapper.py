import yaml
import psycopg
from psycopg import sql
import pandas as pd

class Postgres:
    def __init__(self, host=None, port=None, database=None, user=None, password=None):
        # fill missing from config.yaml
        if not all([host, port, database, user, password]):
            with open("config.yaml", "r", encoding="utf-8") as f:
                pg = (yaml.safe_load(f) or {}).get("postgres", {})
            host     = host     or pg.get("host", "127.0.0.1")
            port     = port     or pg.get("port", 5432)
            database = database or pg.get("database", "postgres")
            user     = user     or pg.get("user", "postgres")
            password = password or pg.get("password", "")

        self.conn = psycopg.connect(
            host=host, port=port, dbname=database, user=user, password=password
        )
        self.conn.autocommit = True  # convenient for simple reads/writes

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def __enter__(self): return self
    def __exit__(self, *exc): self.close()

    def query_df(self, query: str, params=None) -> pd.DataFrame:
        with self.conn.cursor() as cur:
            cur.execute(query, params or ())
            rows = cur.fetchall()
            cols = [c.name for c in cur.description]
        return pd.DataFrame(rows, columns=cols)

    def query_simple(self, schema=None, table=None, fields="*", where=None, params=None, limit=None) -> pd.DataFrame:
        if fields == "*":
            fields_sql = sql.SQL("*")
        elif isinstance(fields, (list, tuple)):
            fields_sql = sql.SQL(", ").join(sql.Identifier(c) for c in fields)
        else:
            fields_sql = sql.SQL(fields)

        from_sql = (
            sql.SQL(" FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
            if schema else sql.SQL(" FROM {}").format(sql.Identifier(table))
        )
        where_sql = sql.SQL("")
        if where: where_sql = sql.SQL(" WHERE ") + sql.SQL(where)
        limit_sql = sql.SQL("")
        if isinstance(limit, int): limit_sql = sql.SQL(" LIMIT {}").format(sql.Literal(limit))

        q = sql.SQL("SELECT ").join([sql.SQL(""), fields_sql]) + from_sql + where_sql + limit_sql
        with self.conn.cursor() as cur:
            cur.execute(q, params or ())
            rows = cur.fetchall()
            cols = [c.name for c in cur.description]
        return pd.DataFrame(rows, columns=cols)