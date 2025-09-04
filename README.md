# DuckLake Provider for Apache Airflow

This is a custom provider for integrating DuckLake (based on DuckDB) with Apache Airflow.

## DuckLake Configuration
The DuckLakeHook uses Airflow connection fields and extras to configure the connection. Standard fields are relabeled for common use:
- Host: Used for metadata host (e.g., Postgres/MySQL host) or file path (e.g., for DuckDB/SQLite metadata file).
- Login: Username (for Postgres/MySQL).
- Password: Password (for Postgres/MySQL).
- Schema: Metadata schema (defaults to 'duckdb').
- Extra: JSON dict for all other settings (required for engine, storage_type, and conditional fields).

Example extras JSON (adjust based on engine and storage_type):
```json
{
  "engine": "postgres",
  "dbname": "my_ducklake",
  "pgdbname": "dev_nophiml_db",
  "storage_type": "s3",
  "s3_bucket": "your-s3-bucket",
  "s3_path": "your/s3/path/",
  "aws_access_key_id": "your-access-key-id",
  "aws_secret_access_key": "your-secret-access-key",
  "aws_region": "us-east-1",
  "install_extensions": ["spatial"],  # Optional: Inherited from DuckDB provider
  "load_extensions": ["spatial"]      # Optional
}
```
### Supported Engines (set in extras['engine'])
- duckdb: Requires 'metadata_file' in extras or host as file path.
- sqlite: Requires 'metadata_file' in extras or host as file path.
- postgres: Requires host, login, password, and 'pgdbname' in extras.
- mysql: Requires host, login, password, and 'mysqldbname' in extras.

### Supported Storage Types (set in extras['storage_type'], default 's3')
- s3: Requires 's3_bucket', 's3_path'; optional AWS creds.
- azure: Requires 'azure_account_name', 'azure_container', 'azure_path'; optional connection_string.
- gcs: Requires 'gcs_bucket', 'gcs_path'; optional service_account_key (JSON string).
- local: Requires 'local_data_path'.

The UI shows core fields; use extras for engine/storage-specific ones. For dynamic behavior, select engine/storage in extras and provide corresponding keys.
