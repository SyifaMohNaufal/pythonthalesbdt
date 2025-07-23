# Python Thales BDT

**PythonThalesBDT** is a Python-based batch data transformer that integrates with **Thales VTS (Vault Tokenization Server)** for secure field-level data tokenization and detokenization. This tool pulls data from a source DB, processes sensitive fields via HTTP APIs, and pushes the output to a destination DB using SQLAlchemy.

## Prerequisites
Ensure you have the following installed before proceeding:
- Python 3.x
- `pip` (Python package manager)
- `virtualenv` (for environment isolation)

## Installation

```bash
git clone https://github.com/SyifaMohNaufal/pythonthalesbdt.git
cd pythonthalesbdt
pip install -r requirements.txt
````


## ⚙Configuration

### 1. Policy File (`*.policy`)

Defines:

* DB connection info
* Schema and table mappings
* Columns to tokenize or detokenize

#### Example:

```json
{
  "source": {
    "connectionurl": "postgresql+psycopg2://user:pass@localhost:5432/source_db"
  },
  "destination": {
    "connectionurl": "mssql+pyodbc://user:pass@localhost:1433/dest_db?driver=ODBC+Driver+17+for+SQL+Server"
  },
  "tables": [
    {
      "sourceSchema": "public",
      "destinationSchema": "dbo",
      "sourceTable": "raw_users",
      "destinationTable": "tokenized_users",
      "columns": [
        {
          "name": "email",
          "action": "TOKENIZE",
          "config": [{
            "tokenGroup": "default",
            "tokenTemplate": "email-template"
          }]
        },
        {
          "name": "ssn",
          "action": "DETOKENIZE",
          "config": [{
            "tokenGroup": "default",
            "tokenTemplate": "ssn-template"
          }]
        }
      ]
    }
  ]
}
```

## Database Connection String Templates
These are database connection strings template of supported database

| Database                      | SQLAlchemy Dialect    | Connection String Format                                                         |
| ----------------------------- | --------------------- | -------------------------------------------------------------------------------- |
| PostgreSQL                    | `postgresql+psycopg2` | `postgresql+psycopg2://user:pass@host:5432/dbname`                               |
| SQL Server                    | `mssql+pyodbc`        | `mssql+pyodbc://user:pass@host:1433/dbname?driver=ODBC+Driver+17+for+SQL+Server` |
| SQLite (file)                 | `sqlite`              | `sqlite:///path/to/file.db`                                                      |
| SQLite (memory)               | `sqlite`              | `sqlite:///:memory:`                                                             |
| MySQL                         | `mysql+pymysql`       | `mysql+pymysql://user:pass@host:3306/dbname`                                     |                                    |
| MariaDB                       | `mariadb+pymysql`     | `mariadb+pymysql://user:pass@host:3306/dbname`                                   |

---

### 2. Config File (`bdt.config`)

Stores global configuration for VTS API access and batching.

```json
{
  "vts": {
    "userName": "apiuser",
    "password": "apipass",
    "tokenUrl": "https://vts.example.com/vts/tokenize",
    "detokenUrl": "https://vts.example.com/vts/detokenize"
  },
  "batchSize": 1000
}
```

---

## Usage

```bash
python bdt_v4.py -p your_policy.policy -c bdt.config
```



## Log Output Example

```
[INFO] 2025-07-22 10:00:01 Row 0 - Start processing
    [TOKENIZE] email → abc123xyz
[INFO] 2025-07-22 10:00:02 Row 0 - Processed successfully
```

---

## How It Works

1. Reads data from the source DB
2. Applies tokenization or detokenization
3. Logs and tracks each row's outcome
4. Inserts valid rows into the destination DB

---

## Retry and Error Handling

* API calls use retry with exponential backoff
* Failed rows are logged with details
* You can safely rerun the script if partial rows fail

---

## Testing

You can test using:

* SQLite or PostgreSQL locally
* A mock Thales VTS endpoint
* Debug logs from each tokenized/detokenized row

---

## Contributing

1. Fork this repo
2. Create a new branch (`git checkout -b feature/thing`)
3. Commit and push (`git commit -m "Add cool thing"`)
4. Open a Pull Request

---

## License

MIT License — see the [LICENSE](LICENSE) file for full text.

---

## Author

**Syifa Mohammad Naufal**
GitHub: [@SyifaMohNaufal](https://github.com/SyifaMohNaufal)
