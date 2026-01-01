# InsightsHub Ingestion

A metadata-driven, declarative pyspark ETL framework for building medallion architecture data pipelines.

Designed to run seamlessly on both local development environments and Databricks.

## Architecture

The framework consists of **three core modules** that work together to provide a complete ETL solution:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Ingestion Framework                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌───────────────────┐   ┌───────────────────────────┐   ┌──────────────────┐   │
│  │     LOADCORE      │   │           SRC             │   │  CUSTOM_LOGGER   │   │
│  │                   │   │                           │   │                  │   │
│  │  ┌─────────────┐  │   │  ┌─────────────────────┐  │   │  ┌────────────┐  │   │
│  │  │ Environment │──┼───┼─>│       Runner        │  │   │  │   JSON     │  │   │
│  │  │  Detection  │  │   │  │   (Orchestrator)    │  │   │  │ Formatter  │  │   │
│  │  └─────────────┘  │   │  └──────────┬──────────┘  │   │  └────────────┘  │   │
│  │  ┌─────────────┐  │   │             │             │   │  ┌────────────┐  │   │
│  │  │    Spark    │  │   │  ┌──────────▼──────────┐  │   │  │   Delta    │  │   │
│  │  │   Manager   │──┼───┼─>│      Pipeline       │  │   │  │  Handler   │  │   │
│  │  └─────────────┘  │   │  │     (ETL Logic)     │  │   │  └────────────┘  │   │ 
│  │  ┌─────────────┐  │   │  └──────────┬──────────┘  │   │  ┌────────────┐  │   │
│  │  │   Secrets   │  │   │             │             │   │  │   Async    │  │   │
│  │  │   Manager   │  │   │  ┌──────────▼──────────┐  │   │  │   Queue    │  │   │
│  │  └─────────────┘  │   │  │ Readers  │ Writers  │  │   │  └────────────┘  │   │
│  │  ┌─────────────┐  │   │  │ Registry │ Registry │  │   │                  │   │
│  │  │   Config    │  │   │  └─────────────────────┘  │   │                  │   │
│  │  │   Loader    │  │   │  ┌─────────────────────┐  │   │                  │   │
│  │  └─────────────┘  │   │  │     Transforms      │  │   │                  │   │
│  │                   │   │  └─────────────────────┘  │   │                  │   │
│  └───────────────────┘   └───────────────────────────┘   └──────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Documentation

| Module Readme |
|----------|
| [Logging Guide](custom_logger/README.md) |
| [Loadcore Guide](loadcore/README.md) |
| [Src framework Guide](src/README.md) |

## 1. Logger

The **observability layer** that captures structured logs for debugging and monitoring.

Provides queryable, structured logs that work consistently across local and Databricks environments.

| Component | Purpose |
|-----------|---------|
| **JSON Formatter** | Converts log records to JSON format |
| **Delta Handler** | Writes logs to a Delta table for SQL-based log analysis |
| **Async Queue** | Non-blocking log handling via `QueueHandler` to prevent I/O bottlenecks |

## 2. Loadcore

The **initialisation and configuration layer** that bootstraps the framework.

Ensures the same pipeline code runs unchanged in both local development and Databricks production environments.

| Component | Purpose |
|-----------|---------|
| **Environment** | Auto-detects local vs Databricks runtime, coordinates initialisation |
| **Spark Manager** | Creates local Spark sessions with Delta Lake support or retrieves active Databricks sessions |
| **Secrets Manager** | Loads secrets from `.env` (local) or Azure Key Vault secret scopes (Databricks) |
| **Config Loader** | Parses `env.config.yaml` and provides type-safe configuration dataclasses |


## 3. SRC

The **pipeline execution engine** that orchestrates data movement through the medallion architecture.

Provide a declarative, decorator-based approach to defining pipelines with automatic input/output handling.

| Component | Purpose |
|-----------|---------|
| **Runner** | Orchestrates the full ETL lifecycle: read -> transform -> write |
| **Pipeline** | Base class for ETL logic with access to inputs, outputs |
| **Reader Registry** | Maps input table definitions to reader implementations via decorators |
| **Writer Registry** | Maps output table definitions to writer implementations via decorators |
| **Readers** | `SparkDataFrameReader`, `DeltaTableReader`, `CatalogTableReader`, `AutoLoaderReader` |
| **Writers** | `SparkDataFrameWriter`, `DeltaMergeWriter`, `DeltaMergeAutoPartitionWriter` |
| **Transforms** | Reusable transformations like `apply_schema` for schema enforcement |

## Project Structure

```
ih-ingestion/
├── loadcore/                 # Environment and configuration management
│   ├── environment.py        # Auto-detect env, initialise Spark/logging
│   ├── config.py             # Configuration dataclasses
│   ├── secrets.py            # Remote secret loading (Databricks scope)
│   ├── spark_manager.py      # Spark session builders
│   └── README.md             # Loadcore documentation
│
├── src/                      # Core framework
│   ├── core/                 # Pipeline, Runner, Registries
│   ├── readers/              # Data readers (Spark, Delta, Catalog, AutoLoader)
│   ├── writers/              # Data writers (Spark, Delta Merge)
│   ├── transforms/           # Data transformations
│   └── README.md             # Framework documentation + pipeline onboarding
│
├── pipelines/                # Pipeline implementations
│   ├── landed/               # Raw data ingestion layer
│   ├── bronze/               # Cleansed data layer
│   ├── silver/               # Conformed data layer
│   └── gold/                 # Aggregated/business layer
│
├── custom_logger/            # Logging configuration
│   ├── config.yaml           # Logging handlers config
│   ├── json_formatter.py     # JSON log formatter
│   ├── delta_handler.py      # Delta table log handler
│   └── README.md             # Logging documentation
│
├── env.config.yaml           # Environment configuration
├── .env                      # Local secrets (git-ignored)
├── .env.example              # Template for required secrets
└── demo.py                   # Example pipeline execution
```

## Local Installation

Follow these steps to set up the local development environment:

1. **Clone the repository**:
   ```bash
   mkdir -p ~/dev/repos/zebragroup
   cd ~/dev/repos/zebragroup
   git clone <repository-url>
   cd ih-ingestion
   ```

2. **Start the Docker container**:
   ```bash
   cd .docker
   docker-compose up --build -d
   ```

3. **Attach to the container in VSCode**:
   - Install the **Dev Containers** extension in VSCode
   - Open the Docker extension panel
   - Right-click on the `ih-ingestion` container and select **Attach to Container**

4. **Open the workspace**:
   - Once attached, open the `/workspace` folder

5. **Configure Git settings** (inside the container):
   ```bash
   git config core.filemode false
   git config core.autocrlf true
   ```
