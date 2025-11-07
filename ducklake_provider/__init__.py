def get_provider_info():
    return {
        "package-name": "airflow-provider-ducklake",
        "name": "DuckLake",
        "description": "DuckLake provider for Apache Airflow (based on DuckDB)",
        "hook-class-names": ["ducklake_provider.hooks.ducklake_hook.DuckLakeHook"],
        "versions": ["0.0.7"],
        "connection-types": [
            {
                "hook-class-name": "ducklake_provider.hooks.ducklake_hook.DuckLakeHook",
                "connection-type": "ducklake",
            }
        ],
    }
