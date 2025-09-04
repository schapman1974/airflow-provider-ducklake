from __future__ import annotations
from typing import Dict, List
import logging

import duckdb

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

class DuckLakeHook(DbApiHook):
    """Interact with DuckLake (based on DuckDB), with support for various engines and storage types via connection fields and extras."""

    conn_name_attr = "ducklake_conn_id"
    default_conn_name = "ducklake_default"
    conn_type = "ducklake"
    hook_name = "DuckLake"
    placeholder = "?"

    def get_conn(self) -> duckdb.DuckDBPyConnection:
        """Sets up a DuckDB connection and attaches DuckLake using Airflow connection configs."""
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)

        # Extract configs from connection fields and extras
        extra = airflow_conn.extra_dejson()  # Parses extra as dict

        # Core configs (map from connection fields or extras)
        engine = extra.get("engine")
        host = airflow_conn.host or extra.get("host", "")
        username = airflow_conn.login or extra.get("username", "")
        password = airflow_conn.password or extra.get("password", "")
        dbname = extra.get("dbname", "my_ducklake")
        schema = airflow_conn.schema or extra.get("schema", "duckdb")
        metadata_file = extra.get("metadata_file") or host  # Use host as fallback for file-based
        pgdbname = extra.get("pgdbname", "")
        mysqldbname = extra.get("mysqldbname", "")
        storage_type = extra.get("storage_type", "s3")

        # Storage-specific configs from extras
        s3_bucket = extra.get("s3_bucket", "")
        s3_path = extra.get("s3_path", "")
        aws_access_key_id = extra.get("aws_access_key_id", "")
        aws_secret_access_key = extra.get("aws_secret_access_key", "")
        aws_region = extra.get("aws_region", "us-east-1")

        azure_account_name = extra.get("azure_account_name", "")
        azure_container = extra.get("azure_container", "")
        azure_path = extra.get("azure_path", "")
        azure_connection_string = extra.get("azure_connection_string", "")

        gcs_bucket = extra.get("gcs_bucket", "")
        gcs_path = extra.get("gcs_path", "")
        gcs_service_account_key = extra.get("gcs_service_account_key", "")

        local_data_path = extra.get("local_data_path", "")

        # Inherited DuckDB extensions from extras (optional)
        install_extensions: List[str] = extra.get("install_extensions", [])
        load_extensions: List[str] = extra.get("load_extensions", [])

        if not engine:
            raise AirflowException("Engine must be specified in extras['engine'] (e.g., 'postgres', 'mysql', 'duckdb', 'sqlite').")

        # Validate required vars based on storage_type
        if storage_type == 's3' and not (s3_bucket and s3_path):
            raise AirflowException("For 's3' storage, 's3_bucket' and 's3_path' are required in extras.")
        elif storage_type == 'azure' and not (azure_account_name and azure_container and azure_path):
            raise AirflowException("For 'azure' storage, 'azure_account_name', 'azure_container', and 'azure_path' are required in extras.")
        elif storage_type == 'gcs' and not (gcs_bucket and gcs_path):
            raise AirflowException("For 'gcs' storage, 'gcs_bucket' and 'gcs_path' are required in extras.")
        elif storage_type == 'local' and not local_data_path:
            raise AirflowException("For 'local' storage, 'local_data_path' is required in extras.")

        # Create DuckDB connection
        conn = duckdb.connect()

        try:
            # Base install/load commands (including optional extensions)
            connect_stack = [
                "INSTALL httpfs;",
                "LOAD httpfs;",
                "INSTALL aws;",
                "LOAD aws;",
                "INSTALL ducklake;",
                "LOAD ducklake;",
            ]

            # Install/load user-specified extensions (inherited from DuckDB)
            for ext in install_extensions:
                conn.install_extension(ext)
            for ext in load_extensions:
                conn.load_extension(ext)

            # Engine-specific install/load
            if engine in ['postgres', 'mysql', 'sqlite']:
                connect_stack.extend([f"INSTALL {engine};", f"LOAD {engine};"])

            # Storage-specific secret
            storage_secret = ""
            if storage_type == 's3':
                if aws_access_key_id and aws_secret_access_key:
                    storage_secret = f"""
                        CREATE OR REPLACE SECRET storage_secret (
                            TYPE S3,
                            KEY_ID '{aws_access_key_id}',
                            SECRET '{aws_secret_access_key}',
                            REGION '{aws_region}'
                        );
                    """
                else:
                    storage_secret = "CREATE OR REPLACE SECRET storage_secret (TYPE S3, PROVIDER credential_chain);"
            elif storage_type == 'azure':
                if azure_connection_string:
                    storage_secret = f"""
                        CREATE OR REPLACE SECRET storage_secret (
                            TYPE AZURE,
                            CONNECTION_STRING '{azure_connection_string}'
                        );
                    """
                else:
                    storage_secret = "CREATE OR REPLACE SECRET storage_secret (TYPE AZURE, PROVIDER credential_chain);"
            elif storage_type == 'gcs':
                if gcs_service_account_key:
                    storage_secret = f"""
                        CREATE OR REPLACE SECRET storage_secret (
                            TYPE GCS,
                            SERVICE_ACCOUNT_KEY '{gcs_service_account_key.replace("'", "''")}'  # Escape single quotes
                        );
                    """
                else:
                    storage_secret = "CREATE OR REPLACE SECRET storage_secret (TYPE GCS, PROVIDER credential_chain);"
            # No secret for 'local'

            if storage_secret:
                connect_stack.append(storage_secret)

            # Engine-specific secret (for DB engines like Postgres/MySQL)
            if engine in ['postgres', 'mysql']:
                connect_stack.append(
                    f"CREATE OR REPLACE SECRET db_secret (TYPE {engine.upper()}, HOST '{host}', USER '{username}', PASSWORD '{password}');"
                )

            # Set threads
            connect_stack.append("SET threads to 4;")

            # Build data_path based on storage_type
            if storage_type == 's3':
                data_path = f"s3://{s3_bucket}/{s3_path}"
            elif storage_type == 'azure':
                data_path = f"az://{azure_account_name}/{azure_container}/{azure_path}"
            elif storage_type == 'gcs':
                data_path = f"gs://{gcs_bucket}/{gcs_path}"
            elif storage_type == 'local':
                data_path = local_data_path
            else:
                raise AirflowException(f"Unsupported storage_type: {storage_type}")

            # Engine-specific ATTACH command
            if engine == 'duckdb':
                attach_cmd = f"ATTACH 'ducklake:{metadata_file}' AS {dbname} (DATA_PATH '{data_path}', METADATA_SCHEMA '{schema}');"
            elif engine == 'sqlite':
                attach_cmd = f"ATTACH 'ducklake:sqlite:{metadata_file}' AS {dbname} (DATA_PATH '{data_path}', METADATA_SCHEMA '{schema}');"
            elif engine == 'postgres':
                attach_cmd = f"ATTACH 'ducklake:postgres:dbname={pgdbname} host={host}' AS {dbname} (DATA_PATH '{data_path}', METADATA_SCHEMA '{schema}');"
            elif engine == 'mysql':
                attach_cmd = f"ATTACH 'ducklake:mysql:db={mysqldbname} host={host}' AS {dbname} (DATA_PATH '{data_path}', METADATA_SCHEMA '{schema}');"
            else:
                raise AirflowException(f"Unsupported engine: {engine}")

            connect_stack.append(attach_cmd)
            connect_stack.append(f"USE {dbname};")

            # Execute each command
            for command in connect_stack:
                command = command.strip()
                if command:
                    conn.execute(command)

            logger.info(f"Connected to DuckLake with engine '{engine}' and storage '{storage_type}': {data_path}")
            return conn

        except Exception as e:
            logger.error(f"Failed to setup DuckLake connection: {e}")
            conn.close()
            raise AirflowException(f"Failed to setup DuckLake connection: {str(e)}")

    def get_uri(self) -> str:
        """Override for get_sqlalchemy_engine() - placeholder for DuckLake (not fully URI-based)."""
        return "ducklake:///"

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Custom UI field behavior: Relabel fields for DuckLake use; use extras for engine/storage specifics."""
        return {
            "hidden_fields": ["port"],  # Hide irrelevant fields
            "relabeling": {
                "host": "Metadata Host/File (e.g., DB host or metadata file path)",
                "login": "Username (for Postgres/MySQL)",
                "password": "Password (for Postgres/MySQL)",
                "schema": "Metadata Schema (default: duckdb)",
            },
        }
