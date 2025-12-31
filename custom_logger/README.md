# Logging System

The IH Ingestion Framework includes a comprehensive logging system with multiple output channels: console, JSON files, and optional Delta table storage.

## Architecture Overview

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│   Application   │────>│   QueueHandler   │────>│   stderr (console)  │
│    Logging      │     │   (async queue)  │     │   file_json (JSON)  │
└─────────────────┘     └──────────────────┘     └─────────────────────┘
                                                           │
                        ┌──────────────────┐               │
                        │  DeltaLogHandler │<──────────────┘
                        │                  │
                        └────────┬─────────┘
                                 │
                        ┌────────▼─────────┐
                        │   Delta Table    │
                        │  {catalog}.logs  │
                        │   .app_logs      │
                        └──────────────────┘
```

## Components

### 1. JSON Formatter (`json_formatter.py`)

Converts log records to machine-readable JSON format for structured logging and log aggregation tools.

**Features:**
- ISO 8601 timestamp formatting
- Configurable field mapping via `fmt_keys`
- Preserves exception and stack trace information
- `NonErrorFilter` for restricting handlers to INFO level and below

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
}
```

**Usage:**
```python
from custom_logger.json_formatter import JSONFormatter

formatter = JSONFormatter(
    fmt_keys={
        "level": "levelname",
        "message": "message",
        "timestamp": "timestamp",
        "logger": "name",
    }
)
```

### 2. Delta Log Handler (`delta_handler.py`)

Writes log records to a Delta table for persistent, queryable log storage. Ideal for production environments where you need to analyse logs using SQL.

**Features:**
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

**Default Configuration:**
```yaml
version: 1
disable_existing_loggers: false

formatters:
  simple:
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  json:
    "()" : "custom_logger.json_formatter.JSONFormatter"
    fmt_keys:
      level: levelname
      message: message
      timestamp: timestamp
      logger: name
      module: module
      function: funcName
      line: lineno
      thread_name: threadName

filters:
  non_error:
    "()" : "custom_logger.json_formatter.NonErrorFilter"

handlers:
  stderr:
    class: logging.StreamHandler
    level: DEBUG
    formatter: simple
    stream: ext://sys.stderr

  file_json:
    class: logging.handlers.RotatingFileHandler
    level: DEBUG
    formatter: json
    filename: ./logs/app.log.jsonl
    maxBytes: 10485760  # 10MB
    backupCount: 3
    encoding: utf-8

  queue_handler:
    class: logging.handlers.QueueHandler
    handlers:
      - stderr
      - file_json
    respect_handler_level: true

root:
  level: DEBUG
  handlers:
    - queue_handler
```

**Key Features:**
- **QueueHandler**: Async logging to prevent I/O blocking
- **RotatingFileHandler**: Auto-rotation at 10MB with 3 backups
- **Dual output**: Console (simple format) + JSON file (structured)

## Integration with Environment

The logging system is automatically initialised when you create an `Environment` instance:

```python
from loadcore.environment import Environment

# Logging is set up automatically
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

### Adding Custom Loggers

```yaml
loggers:
  src.core.runner:
    level: DEBUG
    handlers:
      - queue_handler
    propagate: false

  pyspark:
    level: WARNING
    handlers:
      - queue_handler
    propagate: false
```

### Disabling Delta Logging

Delta logging is optional and only activates if explicitly configured. To use it:

```python
# In your pipeline runner
from custom_logger.delta_handler import DeltaLogHandler

handler = DeltaLogHandler(
    spark=config.spark,
    table_name=f"{config.catalog}.logs.app_logs",
)
logging.getLogger().addHandler(handler)
```

## Best Practices

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
   ```

5. **Monitor log size**: Set up alerts for log growth in production

## Troubleshooting

### Logs not appearing
- Check log level settings in `config.yaml`
- Verify the log directory exists and is writable
- Ensure `Environment` is initialised before logging

### Delta table not created
- Verify Spark session has write permissions
- Check catalog and schema exist
- Ensure Delta Lake is properly configured

### Performance issues
- Increase `buffer_size` in `DeltaLogHandler`
- Reduce log verbosity in production
- Use `NonErrorFilter` to limit console output
