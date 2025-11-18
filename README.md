# mssql-mcp-server
Simple MCP server with tools to execute queries against a MSSQL database.

## Requirements
- Python 3.11+
- [pymssql](https://pymssql.readthedocs.io/en/stable/) and `python-dotenv` (installed through `requirements.txt`)
- Network access to a Microsoft SQL Server instance

## Setup
1. (Optional but recommended) create and activate a virtual environment. Example:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and fill in the values for your environment (see below).

## Configuration
The server reads its connection info from environment variables or a `.env` file located beside `mssql_mcp_server.py`.

| Variable | Required | Description |
| --- | --- | --- |
| `MSSQL_SERVER` | ✅ | Hostname or `host,port` of the SQL Server instance |
| `MSSQL_DB` | ❌ | Database name; defaults to `master` when omitted |
| `MSSQL_USER` | ✅ | SQL Server login |
| `MSSQL_PASSWORD` | ✅ | Password for the login |

For convenience, start with `.env.example`:
```bash
cp .env.example .env
```
then edit `.env` with the correct values before running the server.

## Running the server
Launch the FastMCP server with:
```bash
python mssql_mcp_server.py
```

The MCP server exposes two tools:
- `run_query(sql: str)` executes any SQL statement and returns rows or the number of affected rows.
- `describe_table(schema: str, table_name: str)` returns the column metadata using `INFORMATION_SCHEMA`.

Point your MCP-compatible client at the running process to issue SQL queries through these tools.

### Codex sample config
If you are wiring this server into Codex, add a section like the following to your Codex config file (typically `.codex/config.toml`). Example:

```toml
[mcp_servers.mssql-db]
command = "/path/to/project/.venv/bin/python"
args = ["/path/to/project/mssql_mcp_server.py"]
```
