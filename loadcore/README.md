# Loadcore - Environment and Configuration Management

Loadcore is the initialisation and configuration layer of the IH Ingestion Framework.

It handles environment detection, Spark session management, secret loading, and provides a unified configuration interface for pipelines.

## Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Environment                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────┐  │
│  │   Config    │  │   Secrets   │  │    Spark    │  │  Logging   │  │
│  │   Loader    │  │   Manager   │  │   Manager   │  │   Setup    │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  └────────────┘  │
│         │                │                │                │        │
│         └────────────────┴────────────────┴────────────────┘        │
│                                │                                    │
│                          PipelineConfig                             │
└─────────────────────────────────────────────────────────────────────┘
```

## Configuration File

The `env.config.yaml` file defines environment-specific settings:

```yaml
# env.config.yaml
local:
  catalog: "spark_catalog"
  secrets_path: "./secrets.yaml"
  volumes:
    lake: "./data/lake"
    catalog: "./data/catalog"

remote:
  catalog: "unity_catalog"
  secret_scope: "kvss-production-weu"
  volumes:
    lake: "/Volumes/lake"
    catalog: "/Volumes/catalog"
```

### Configuration Properties

| Property | Environment | Description |
|----------|-------------|-------------|
| `catalog` | Both | Spark/Unity Catalog name |
| `secrets_path` | Local | Path to local secrets YAML |
| `secret_scope` | Remote | Databricks secret scope name |
| `volumes` | Both | Named volume path mappings |
| `volumes.lake` | Both | Path to raw files and landed data |
| `volumes.catalog` | Both | Path for unmanaged Delta tables (also used as Spark warehouse locally) |

## Components

### 1. Environment (`environment.py`)

The main entry point for framework initialisation. Handles auto-detection of the runtime environment and coordinates all initialisation steps.

**Key Features:**
- **Auto-detection**: Identifies local vs Databricks runtime
- **Unified initialisation**: Single entry point for all setup
- **Pipeline configuration**: Creates `PipelineConfig` for runners

**Usage:**
```python
from loadcore.environment import Environment

# Initialise with config file
env = Environment("./env.config.yaml")

# Create pipeline configuration
config = env.for_pipeline(
    metadata={"name": "my_pipeline", "version": "1.0"},
    static_config={"batch_size": 1000},
    spark_conf={"spark.sql.shuffle.partitions": "200"},
)
```

**Detection Logic:**
```python
def _detect_mode(self) -> str:
    """
    Detect the current environment mode.
    Returns
    -------
    str
        'local' if running locally, 'remote' if running on Databricks.
    """
    ...
```

**Initialisation Flow:**
1. Load YAML configuration
2. Detect runtime mode (local/remote)
3. Create or get Spark session
4. Load and inject secrets
5. Set up logging (Stdout, JSON file, Delta table)

### 2. Configuration (`config.py`)

Dataclasses for type-safe configuration management.

**LocalEnvironmentConfig:**
```python
@dataclass
class LocalEnvironmentConfig:
    catalog: str             # Spark catalog name (e.g., "spark_catalog")
    volumes: dict[str, str]  # Volume name to path mappings
    secrets_path: str        # Path to secrets.yaml
```

**RemoteEnvironmentConfig:**
```python
@dataclass
class RemoteEnvironmentConfig:
    catalog: str             # Unity Catalog name
    secret_scope: str        # Databricks secret scope
    volumes: dict[str, str]  # Volume name to DBFS path mappings
```

**PipelineConfig:**
```python
@dataclass
class PipelineConfig:
    spark: SparkSession    # Active Spark session
    catalog: str           # Catalog name
    volumes: dict[str, str]  # Volume mappings
    metadata: dict[str, Any]  # Runtime metadata
    static_config: dict[str, Any]  # Static configuration
    spark_conf: dict[str, str]  # Spark configuration overrides
```

### 3. Secrets (`secrets.py`)

Secure secret management for both local development and Databricks.

**Secret Dataclass:**
```python
@dataclass
class Secret:
    key: str
    value: str

    def __repr__(self) -> str:
        return f"Secret(key='{self.key}', value='***')"
```

**Local Secrets (`secrets.yaml`):**
```yaml
# secrets.yaml
database_password: !secret ${DB_PASSWORD}
api_key: !secret ${API_KEY}
connection_string: !secret ${CONN_STRING}
```

The `!secret` tag references environment variables. Values are resolved at load time.

**Loading Secrets:**
```python
from loadcore.secrets import load_local_secrets, inject_secrets_to_env

# Load from YAML file
secrets = load_local_secrets("./secrets.yaml")

# Inject into environment variables
inject_secrets_to_env(secrets)
```

**Databricks Secret Scope:**
```python
from loadcore.secrets import load_remote_secrets

# Load from Databricks secret scope
secrets = load_remote_secrets(
    dbutils=dbutils,
    scope_name="my-secret-scope"
)
```

**Security Best Practices:**
- Never commit `secrets.yaml` to version control (add to `.gitignore`)
- Use environment variables for local development
- Use Databricks secret scopes in production
- Secrets are hidden in `__repr__` to prevent accidental logging

### 4. Spark Manager (`spark_manager.py`)

Abstract factory for Spark session creation.

**AbstractSessionBuilder:**
```python
class AbstractSessionBuilder(ABC):
    @abstractmethod
    def get_or_create(self) -> SparkSession:
        """Create or get existing Spark session."""
        pass
```

**LocalSparkSessionBuilder:**
Creates a local Spark session with Delta Lake support. Uses the `catalog` volume path as the Spark warehouse directory:
```python
class LocalSparkSessionBuilder(AbstractSessionBuilder):
    def __init__(self, app_name: str, catalog_path: str):
        self.app_name = app_name
        self.catalog_path = catalog_path

    def get_or_create(self) -> SparkSession:
        return (
            SparkSession.builder
            .appName(self.app_name)
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", self.catalog_path)
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
            .config("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
            .enableHiveSupport()
            .getOrCreate()
        )
```

**RemoteSparkSessionBuilder:**
Gets the active Databricks session:
```python
class RemoteSparkSessionBuilder(AbstractSessionBuilder):
    def get_or_create(self) -> SparkSession:
        return SparkSession.getActiveSession()
```

## Volume Abstraction

Volumes provide environment-agnostic path references:

```python
# In table definitions
class MyTableDef(TableDefinition):
    @property
    def path(self) -> str:
        # Works in both local and Databricks
        lake_path = self.get_volume("lake")
        return f"{lake_path}/raw/orders/orders.csv"
```

**Local Resolution:**
```
get_volume("lake") -> "./data/lake"
```

**Remote Resolution:**
```
get_volume("lake") -> "/Volumes/lake"
```

## Full Initialisation Example

```python
from loadcore.environment import Environment
from src.core.runner import Runner
from pipelines.bronze.pipelines.bronze_orders_pipeline import BronzeOrdersPipeline

# 1. Initialise environment
env = Environment("./env.config.yaml")

# 2. Create pipeline configuration
config = env.for_pipeline(
    metadata={
        "name": "bronze_orders",
        "version": "1.0",
        "run_date": "2025-01-15",
    },
    static_config={
        "batch_size": 10000,
        "enable_dedup": True,
    },
    spark_conf={
        "spark.sql.shuffle.partitions": "200",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
    },
)

# Run pipeline
runner = Runner(config=config, pipeline=BronzeOrdersPipeline)
runner.run()
```

## Environment Detection

The framework automatically detects the runtime environment:

| Condition | Mode | Spark Session |
|-----------|------|---------------|
| `SparkSession.getActiveSession()` returns session | Remote | Uses existing |
| No active session | Local | Creates new |

This allows the same code to run unchanged in both environments:

```python
# Same code works in both Jupyter notebook and Databricks
env = Environment("./env.config.yaml")
config = env.for_pipeline(metadata={"name": "test"})

# In local: Creates new Spark session
# In Databricks: Uses active cluster session
```

## Extending LoadCore

### Custom Session Builder

```python
from loadcore.spark_manager import AbstractSessionBuilder

class CustomSparkSessionBuilder(AbstractSessionBuilder):
    def __init__(self, config: dict):
        self.config = config

    def get_or_create(self) -> SparkSession:
        builder = SparkSession.builder
        for key, value in self.config.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()
```

### Custom Secret Loader

```python
from loadcore.secrets import Secret

def load_from_vault(vault_path: str) -> list[Secret]:
    """Load secrets from HashiCorp Vault."""
    # Custom vault integration
    client = hvac.Client()
    data = client.secrets.kv.read_secret(path=vault_path)
    return [
        Secret(key=k, value=v)
        for k, v in data["data"]["data"].items()
    ]
```

## Best Practices

1. **Use .env variables for local secrets**: Never hardcode sensitive values
2. **Initialise once**: Create `Environment` once at startup
3. **Pass config down**: Use `PipelineConfig` for all pipeline needs
