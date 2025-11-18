import os
from pathlib import Path

import pymssql
from dotenv import load_dotenv
from mcp.server.fastmcp import Context, FastMCP

load_dotenv(Path(__file__).with_name(".env"))

# Set env vars before starting: MSSQL_SERVER, MSSQL_DB (optional), MSSQL_USER, MSSQL_PASSWORD
def build_connection_args() -> dict:
  return {
      "server": os.environ["MSSQL_SERVER"],
      "database": os.environ.get("MSSQL_DB", "master"),
      "user": os.environ["MSSQL_USER"],
      "password": os.environ["MSSQL_PASSWORD"],
      "charset": "UTF-8",
      "login_timeout": 5,
  }

server = FastMCP(name="mssql-db")

@server.tool()
async def run_query(ctx: Context, sql: str):
  """
  Execute arbitrary SQL and return result rows (for queries) or rowcount.
  """
  with pymssql.connect(**build_connection_args()) as conn:
      with conn.cursor(as_dict=True) as cur:
          cur.execute(sql)
          if cur.description:
              rows = cur.fetchall()
              columns = list(rows[0].keys()) if rows else []
              return {"columns": columns, "rows": rows}
          conn.commit()
          return {"rows_affected": cur.rowcount}

@server.tool()
async def describe_table(ctx: Context, schema: str, table_name: str):
  """
  Return column metadata for schema.table_name using INFORMATION_SCHEMA.
  """
  query = """
  SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
  ORDER BY ORDINAL_POSITION;
  """
  with pymssql.connect(**build_connection_args()) as conn:
      with conn.cursor(as_dict=True) as cur:
          cur.execute(query, (schema, table_name))
          return [
              {
                  "column": row["COLUMN_NAME"],
                  "type": row["DATA_TYPE"],
                  "nullable": row["IS_NULLABLE"] == "YES",
                  "length": row["CHARACTER_MAXIMUM_LENGTH"],
              }
              for row in cur.fetchall()
          ]

if __name__ == "__main__":
    server.run()
