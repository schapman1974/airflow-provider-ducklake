from __future__ import annotations
from typing import Dict, List, Optional
import logging
import os

import duckdb

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

MEMORY_PLAN_SETTINGS = {
    "conservative": {"fraction": 0.15, "min_bytes": 256 * 1024**2, "max_bytes": 4 * 1024**3},
    "midtier": {"fraction": 0.25, "min_bytes": 512 * 1024**2, "max_bytes": 16 * 1024**3},
    "aggressive": {"fraction": 0.4, "min_bytes": 1024 * 1024**2, "max_bytes": 32 * 1024**3},
}
DEFAULT_MEMORY_PLAN = "midtier"
ABS_MIN_MEMORY_BYTES = 256 * 1024**2  # Never allocate less than 256MB when possible


def _format_duckdb_memory_limit(num_bytes: int) -> str:
    """Format bytes into DuckDB-compatible memory strings (e.g., 4GB, 512MB)."""
    if num_bytes >= 1024**3:
        gigabytes = max(num_bytes // 1024**3, 1)
        return f"{gigabytes}GB"
    megabytes = max(num_bytes // 1024**2, 1)
    return f"{megabytes}MB"


def _get_available_memory_bytes() -> Optional[int]:
    """Best-effort retrieval of available physical memory in bytes."""
    try:
        import psutil  # type: ignore

        return int(psutil.virtual_memory().available)
    except Exception:
        pass

    # /proc/meminfo provides MemAvailable on most Linux systems
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as meminfo:
            for line in meminfo:
                if line.startswith("MemAvailable:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1]) * 1024  # Value is in kB
    except (OSError, ValueError):
        pass

    # POSIX sysconf fallback
    if hasattr(os, "sysconf"):
        try:
            page_size = os.sysconf("SC_PAGE_SIZE")
            if "SC_AVPHYS_PAGES" in os.sysconf_names:
                avail_pages = os.sysconf("SC_AVPHYS_PAGES")
                if isinstance(page_size, int) and isinstance(avail_pages, int):
                    return page_size * avail_pages
            elif "SC_PHYS_PAGES" in os.sysconf_names:
                total_pages = os.sysconf("SC_PHYS_PAGES")
                if isinstance(page_size, int) and isinstance(total_pages, int):
                    return page_size * total_pages
        except (ValueError, OSError):
            pass

    # Windows fallback via GlobalMemoryStatusEx
    try:
        import ctypes

        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        stat = MEMORYSTATUSEX()
        stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
            return int(stat.ullAvailPhys)
    except Exception:
        pass

    return None


def _determine_memory_limit_bytes(memory_plan: str) -> Optional[int]:
    """Return a memory limit (in bytes) derived from available memory and the requested plan."""
    plan_name = (memory_plan or DEFAULT_MEMORY_PLAN).lower()
    settings = MEMORY_PLAN_SETTINGS.get(plan_name)
    if settings is None:
        raise AirflowException(
            f"Invalid memory_plan '{memory_plan}'. Expected one of {list(MEMORY_PLAN_SETTINGS.keys())}."
        )

    available_bytes = _get_available_memory_bytes()
    if not available_bytes:
        return None

    fraction_target = int(available_bytes * settings["fraction"])
    limit_bytes = max(fraction_target, settings["min_bytes"])
    limit_bytes = min(limit_bytes, settings["max_bytes"])
    limit_bytes = min(limit_bytes, available_bytes)

    if available_bytes >= ABS_MIN_MEMORY_BYTES:
        limit_bytes = max(limit_bytes, ABS_MIN_MEMORY_BYTES)

    return max(limit_bytes, 0) or None

class DuckLakeHook(DbApiHook):
    """Interact with DuckLake (based on DuckDB), with support for various engines and storage types via connection fields and extras."""

    conn_name_attr = "ducklake_conn_id"
    default_conn_name = "ducklake_default"
    conn_type = "ducklake"
    hook_name = "DuckLake"
    placeholder = "?"

    def __init__(
        self,
        *,
        memory_plan: Optional[str] = None,
        memory_limit: Optional[str] = None,
        threads: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._configured_memory_plan = memory_plan
        self._configured_memory_limit = memory_limit
        self._configured_threads = threads

    def get_conn(self) -> duckdb.DuckDBPyConnection:
        """Sets up a DuckDB connection and attaches DuckLake using Airflow connection configs."""
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)

        # Extract configs from connection fields and extras
        extra = airflow_conn.extra_dejson  # Parses extra as dict

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

        # Performance settings: hook kwargs override connection extras
        num_threads = (
            self._configured_threads
            if self._configured_threads is not None
            else extra.get("threads", 4)
        )
        memory_limit = (
            self._configured_memory_limit
            if self._configured_memory_limit is not None
            else extra.get("memory_limit")
        )
        memory_plan = (
            self._configured_memory_plan
            if self._configured_memory_plan is not None
            else extra.get("memory_plan", DEFAULT_MEMORY_PLAN)
        )

        def _coerce_positive_int(value, field_name, default):
            """Coerce Airflow extra values to positive ints while tolerating blanks."""
            if value in (None, ""):
                return default
            try:
                coerced = int(value)
            except (TypeError, ValueError):
                raise AirflowException(f"'{field_name}' must be a positive integer, got: {value}")
            if coerced < 1:
                raise AirflowException(f"'{field_name}' must be a positive integer, got: {value}")
            return coerced

        num_threads = _coerce_positive_int(num_threads, "threads", 4)

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

        # Validations
        if not engine:
            raise AirflowException("Engine must be specified in extras['engine'] (e.g., 'postgres', 'mysql', 'duckdb', 'sqlite').")

        # Normalize memory plan and memory limit handling
        memory_plan = (memory_plan or DEFAULT_MEMORY_PLAN).lower()
        if memory_plan not in MEMORY_PLAN_SETTINGS:
            raise AirflowException(
                f"Invalid memory_plan '{memory_plan}'. Expected one of {list(MEMORY_PLAN_SETTINGS.keys())}."
            )

        if memory_limit is not None:
            memory_limit = str(memory_limit).strip()
            if not memory_limit:
                memory_limit = None

        auto_memory_bytes: Optional[int] = None
        if memory_limit is None:
            auto_memory_bytes = _determine_memory_limit_bytes(memory_plan)
            if auto_memory_bytes:
                memory_limit = _format_duckdb_memory_limit(auto_memory_bytes)
                logger.info(
                    "Auto-selected DuckDB memory limit %s using '%s' memory plan.",
                    memory_limit,
                    memory_plan,
                )
            else:
                logger.info(
                    "Unable to determine available physical memory; DuckDB memory_limit will use the engine default."
                )

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
            default_connect_stack: List[str] = [
                "INSTALL httpfs;",
                "LOAD httpfs;",
                "INSTALL aws;",
                "LOAD aws;",
                "INSTALL ducklake;",
                "LOAD ducklake;",
            ]
            user_connect_stack = extra.get("connect_stack")
            if user_connect_stack is not None:
                if not isinstance(user_connect_stack, list) or not all(isinstance(cmd, str) for cmd in user_connect_stack):
                    raise AirflowException("extras['connect_stack'] must be a list of SQL command strings.")
                connect_stack = user_connect_stack.copy()
            else:
                connect_stack = default_connect_stack.copy()

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
                    f"CREATE OR REPLACE SECRET (TYPE {engine}, HOST '{host}', USER '{username}', PASSWORD '{password}');"
                )

            # Set threads and optional memory limit
            connect_stack.append(f"SET threads TO {num_threads};")
            if memory_limit:
                sanitized_memory = memory_limit.replace("'", "''")
                connect_stack.append(f"SET memory_limit='{sanitized_memory}';")

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

            if memory_limit:
                source = "auto" if auto_memory_bytes else "manual"
                memory_log = f", memory_limit: {memory_limit} ({source})"
            else:
                memory_log = f", memory_plan: {memory_plan}"
            logger.info(
                f"Connected to DuckLake with engine '{engine}' and storage '{storage_type}': {data_path} (threads: {num_threads}{memory_log})"
            )
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
