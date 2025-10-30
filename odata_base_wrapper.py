import requests
import pandas as pd
import re

BASE_URL = ""

class OData:
    def __init__(self, url: str):
        if not url:
            raise ValueError("URL must be provided (pass one or subclass OData).")
        self.base_url = url.rstrip("/")
        self.session = requests.Session()

    def close(self):
        self.session.close()

    # ---------- internals ----------

    def _sql_where_to_odata(self, where_sql: str) -> str | None:
        """
        Convert a simple SQL WHERE into an OData $filter.
        Supports:
          - = != <> < <= > >=  →  eq ne lt le gt ge
          - AND / OR
          - IS NULL / IS NOT NULL
          - IN (...) / NOT IN (...)
          - LIKE '%x', 'x%', '%x%'  → endswith/startswith/contains
          - Keeps numbers unquoted; quotes strings safely
        """
        s = where_sql.strip()
        # remove trailing ORDER BY / LIMIT if present (handled elsewhere)
        s = re.split(r"\bORDER\s+BY\b|\bLIMIT\b", s, flags=re.IGNORECASE)[0].strip()

        def odata_lit(tok: str) -> str:
            t = tok.strip()
            # already quoted?
            if re.fullmatch(r"'([^']|'')*'", t):
                inner = t[1:-1].replace("'", "''")
                return f"'{inner}'"
            # number?
            if re.fullmatch(r"\d+(?:\.\d+)?", t):
                return t
            # booleans / null
            low = t.lower()
            if low in ("null", "true", "false"):
                return low
            # fall back: treat as string
            return "{}".format("'" + t.replace("'", "''") + "'")

        # map basic operators and boolean connectors (longest first)
        for pat, rep in [
            (r"(?i)\bIS\s+NOT\s+NULL\b", " ne null"),
            (r"(?i)\bIS\s+NULL\b",      " eq null"),
            (r">=", " ge "),
            (r"<=", " le "),
            (r"<>", " ne "),
            (r"!=", " ne "),
            (r"=",  " eq "),
            (r">",  " gt "),
            (r"<",  " lt "),
            (r"(?i)\bAND\b", " and "),
            (r"(?i)\bOR\b",  " or "),
        ]:
            s = re.sub(pat, rep, s)

        # IN / NOT IN
        def repl_in(m):
            col = m.group("col").strip()
            is_not = bool(m.group("not"))
            vals_raw = m.group("vals")
            # split items respecting quotes
            items = [x.strip() for x in re.findall(r"'(?:[^']|'')*'|[^,]+", vals_raw) if x.strip()]
            lits = [odata_lit(x) for x in items]
            if not lits:
                return "(1 eq 1)" if is_not else "(1 eq 0)"
            if is_not:
                return "(" + " and ".join(f"{col} ne {v}" for v in lits) + ")"
            return "(" + " or ".join(f"{col} eq {v}" for v in lits) + ")"

        s = re.sub(
            r"(?is)(?P<col>[A-Za-z_][A-Za-z0-9_\.]*)\s+(?P<not>NOT\s+)?IN\s*\(\s*(?P<vals>[^)]*)\s*\)",
            repl_in,
            s,
        )

        # LIKE
        def like_cb(m):
            col = m.group("col").strip()
            lit = odata_lit(m.group("lit"))
            inner = lit[1:-1]  # remove quotes
            if inner.startswith("%") and inner.endswith("%"):
                return f"contains({col}, {lit})"
            if inner.endswith("%"):
                return f"startswith({col}, '{inner[:-1]}')"
            if inner.startswith("%"):
                return f"endswith({col}, '{inner[1:]}')"
            return f"{col} eq {lit}"

        s = re.sub(
            r"(?is)(?P<col>[A-Za-z_][A-Za-z0-9_\.]*)\s+LIKE\s+(?P<lit>'(?:[^']|'')*')",
            like_cb,
            s,
        )

        # unquote numeric literals for gt/ge/lt/le
        s = re.sub(
            r"(\b[A-Za-z_][A-Za-z0-9_\.]*\b)\s+(gt|ge|lt|le)\s+'(\d+(?:\.\d+)?)'",
            lambda m: f"{m.group(1)} {m.group(2)} {m.group(3)}",
            s,
        )

        s = re.sub(r"\s+", " ", s).strip()
        return s or None

    def _extract_select_from_where_order_limit(self, query: str):
        """
        Parse a lightweight SQL: SELECT ... FROM table [WHERE ...] [ORDER BY ...] [LIMIT n]
        Returns (fields, table, where_clause, order_by, limit)
        """
        m_fields = re.search(r"SELECT\s+(.*?)\s+FROM\b", query, re.IGNORECASE | re.DOTALL)
        m_from   = re.search(r"\bFROM\s+([A-Za-z_][A-Za-z0-9_\.]*)", query, re.IGNORECASE)
        if not (m_fields and m_from):
            raise ValueError("Invalid query")

        fields = m_fields.group(1).strip()
        table  = m_from.group(1).split(".")[-1]

        # WHERE (until ORDER BY or LIMIT or end)
        m_where = re.search(r"\bWHERE\s+(.+?)(?=\bORDER\s+BY\b|\bLIMIT\b|$)", query, re.IGNORECASE | re.DOTALL)
        where_clause = m_where.group(1).strip() if m_where else None

        # ORDER BY
        m_order = re.search(r"\bORDER\s+BY\s+(.+?)(?=\bLIMIT\b|$)", query, re.IGNORECASE | re.DOTALL)
        order_by = m_order.group(1).strip() if m_order else None

        # LIMIT
        m_limit = re.search(r"\bLIMIT\s+(\d+)\b", query, re.IGNORECASE)
        limit = int(m_limit.group(1)) if m_limit else None

        return fields, table, where_clause, order_by, limit

    # ---------- public ----------

    def query(self, query: str) -> pd.DataFrame:
        """
        Accept a simple SQL-like query, convert to OData params, call the API, and return a DataFrame.
        Supports WHERE with eq/ne/lt/le/gt/ge, AND/OR, IN/NOT IN, LIKE, IS [NOT] NULL.
        Also maps ORDER BY -> $orderby and LIMIT -> $top.
        """
        fields, table, where_clause, order_by, limit = self._extract_select_from_where_order_limit(query)

        # $select: omit if "*"
        select_param = None if fields == "*" else fields

        # $filter
        filter_param = self._sql_where_to_odata(where_clause) if where_clause else None

        # Build params dict without None values
        params = {"$format": "json"}
        if select_param:
            params["$select"] = select_param
        if filter_param:
            params["$filter"] = filter_param
        if order_by:
            # pass through as-is; simple "col [ASC|DESC], col2 ..."
            params["$orderby"] = order_by
        if isinstance(limit, int):
            params["$top"] = limit

        resp = self.session.get(f"{self.base_url}/{table}", params=params)
        resp.raise_for_status()
        return pd.json_normalize(resp.json().get("value", []))