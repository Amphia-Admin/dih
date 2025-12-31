# IH Ingestion Framework

A metadata-driven production-grade PySpark ETL framework for building medallion architecture data pipelines. 
Designed to run seamlessly on both local development environments and Databricks.

## Features

- **Environment Auto-Detection**: Automatically detects local vs Databricks runtime
- **Medallion Architecture**: Built-in support for landed/bronze/silver/gold layers
- **Declarative Pipelines**: Define pipelines using decorators and table definitions
- **Delta Lake Native**: First-class support for Delta tables with merge operations
- **Volume Abstraction**: Environment-agnostic paths via volume mappings
- **Comprehensive Logging**: JSON file logging + optional Delta table logging
- **Schema Enforcement**: Apply and validate schemas with transforms

## Project Structure

```
ih-ingestion/
├── loadcore/                 # Environment and configuration management
│   ├── environment.py        # Auto-detect env, initialise Spark/logging
│   ├── config.py             # Configuration dataclasses
│   ├── secrets.py            # Remote secret loading (Databricks scope)
│   ├── spark_manager.py      # Spark session builders
│   └── README.md             # LoadCore documentation
│
├── src/                      # Core framework
│   ├── core/                 # Pipeline, Runner, Registries
│   ├── readers/              # Data readers (Spark, Delta, Catalog, AutoLoader)
│   ├── writers/              # Data writers (Spark, Delta Merge)
│   ├── transforms/           # Data transformations
│   └── README.md             # Framework documentation + pipeline onboarding
│
├── pipelines/                # Your pipeline implementations
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

## Documentation

| Document | Description |
|----------|-------------|
| [Framework Guide](src/README.md) | Core components, readers, writers, and **pipeline onboarding** |
| [LoadCore Guide](loadcore/README.md) | Environment detection, configuration, secrets, Spark session |
| [Logging Guide](custom_logger/README.md) | JSON logging, Delta table logging, configuration |

## Local Installation

Follow these steps to set up your local development environment:

1. **Clone the repository**:
   ```bash
   mkdir -p ~/dev/repos
   cd ~/dev/repos
   git clone <repository-url>
   cd ih-ingestion
   ```

2. **Start the Docker container**:
   ```bash
   cd .docker
   docker-compose up -d
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
