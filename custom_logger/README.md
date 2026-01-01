# Logging System

Comprehensive logging system with multiple output channels: console, JSON files optional Delta table output.

## Architecture Overview

```
                                              ┌─────────────────────┐
                                         ┌───>│   stderr (console)  │
                                         │    └─────────────────────┘
┌─────────────────┐     ┌──────────────────┐
│   Application   │────>│   QueueHandler   │
│    Logging      │     │   (async queue)  │
└────────┬────────┘     └────────┬─────────┘
         │                       │    ┌─────────────────────┐
         │                       └───>│   file_json (JSON)  │
         │                            └─────────────────────┘
         │
         │              ┌──────────────────┐     ┌─────────────────────┐
         └─────────────>│  DeltaLogHandler │────>│     Delta Table     │
                        │    (optional)    │     │  {catalog}.logs     │
                        └──────────────────┘     └─────────────────────┘
```

## Components

### 1. JSON Formatter (`json_formatter.py`)

Converts log records to JSON format for structured logging.

**Output Format:**
```json
{
  "timestamp": "2025-01-15T10:30:45.123456+00:00",
  "level": "INFO",
  "logger": "src.core.runner",
  "message": "Pipeline execution started",
  "module": "runner",
  "function": "run",
  "line": 45
  ...
}
```

### 2. Delta Log Handler (`delta_handler.py`)

Writes log records to a Delta table for queryable log storage.

- **Buffered writes**: Accumulates records before writing (default: 50 records)
- **Periodic flushing**: Auto-flush at intervals (default: 30 seconds)
- **Thread-safe**: Uses locking for concurrent access
- **Auto-creates table**: Creates the Delta table on first write
- **Graceful shutdown**: Flushes remaining logs via `atexit` registration

**Delta Table Schema:**
| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | TIMESTAMP | Log record timestamp |
| `level` | STRING | Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `logger` | STRING | Logger name |
| `message` | STRING | Log message |
| `module` | STRING | Python module name |
| `function` | STRING | Function name |
| `line` | INTEGER | Line number |
| `thread` | STRING | Thread name |
| `exception` | STRING | Exception traceback (if any) |

**Usage:**
```python
from custom_logger.delta_handler import DeltaLogHandler

handler = DeltaLogHandler(
    spark=spark_session,
    table_name="my_catalog.logs.app_logs",
    buffer_size=50,
    flush_interval=30.0,
)

logger = logging.getLogger("my_app")
logger.addHandler(handler)
```

**Querying Logs:**
```sql
-- Recent errors
SELECT timestamp, logger, message, exception
FROM my_catalog.logs.app_logs
WHERE level = 'ERROR'
  AND timestamp > current_timestamp() - INTERVAL 1 HOUR
ORDER BY timestamp DESC;

-- Pipeline execution summary
SELECT
  date_trunc('hour', timestamp) as hour,
  level,
  count(*) as count
FROM my_catalog.logs.app_logs
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

### 3. Configuration (`config.yaml`)

YAML-based logging configuration using Python's `logging.config.dictConfig`.

## Integration with Loadcore module (Environment object)

Logging is automatically initialised when you create an `Environment` instance:

```python
from loadcore.environment import Environment

env = Environment("./env.config.yaml")
```

**Initialisation Flow:**
1. `Environment.initialise()` calls `_setup_logging()`
2. Loads `custom_logger/config.yaml` via `dictConfig`
3. Creates log directory if needed
4. Sets JSON log filename to `{volume}/logs/app_{timestamp}.log.jsonl`
5. Optionally calls `_setup_delta_logging()` for Delta table logging

## File Locations

| Output | Location |
|--------|----------|
| Console | stderr |
| JSON Logs | `{volume}/logs/app_{timestamp}.log.jsonl` |
| Delta Table | `{catalog}.logs.app_logs` |

## Configuration Options

### Customising Log Levels

Modify `config.yaml` to change log levels:

```yaml
root:
  level: INFO  # Change from DEBUG to INFO for less verbose output

handlers:
  stderr:
    level: WARNING  # Only warnings and above to console
  file_json:
    level: DEBUG    # All levels to JSON file
```

### Disabling Delta Logging

Delta logging is optional and only activates if explicitly configured. To use it:

```python
from custom_logger.delta_handler import DeltaLogHandler

handler = DeltaLogHandler(
    spark=config.spark,
    table_name=f"{config.catalog}.logs.app_logs",
)
logging.getLogger().addHandler(handler)
```

# How to add logging in pipelines framework

1. **Use structured logging**: Include relevant context in log messages
   ```python
   logger.info("Processing batch", extra={"batch_id": batch_id, "record_count": count})
   ```

2. **Set appropriate levels**:
   - `DEBUG`: Detailed diagnostic information
   - `INFO`: General operational messages
   - `WARNING`: Unexpected but handled situations
   - `ERROR`: Failures that prevent operation completion
   - `CRITICAL`: System-wide failures

3. **Use logger hierarchy**: Create loggers for each module
   ```python
   logger = logging.getLogger(__name__)
   ```

4. **Flush on completion**: Ensure all logs are written before exit
   ```python
   logging.shutdown()
