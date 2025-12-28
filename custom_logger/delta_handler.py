"""Delta table logging handler."""

import atexit
import logging
import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_LOG_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("level", StringType(), nullable=False),
    StructField("logger", StringType(), nullable=False),
    StructField("message", StringType(), nullable=False),
    StructField("module", StringType(), nullable=True),
    StructField("function", StringType(), nullable=True),
    StructField("line", IntegerType(), nullable=True),
    StructField("thread", StringType(), nullable=True),
    StructField("exception", StringType(), nullable=True),
])


class DeltaLogHandler(logging.Handler):
    """Logging handler that writes log records to a Delta table."""

    def __init__(
        self,
        spark: SparkSession,
        table_name: str,
        buffer_size: int = 100,
        flush_interval: float = 30.0,
    ) -> None:
        super().__init__()
        self._spark = spark
        self._table_name = table_name
        self._buffer_size = buffer_size
        self._flush_interval = flush_interval
        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None

        self._table_exists = False
        self._start_flush_timer()
        atexit.register(self.flush)

    def _start_flush_timer(self) -> None:
        """Start the periodic flush timer."""
        self._timer = threading.Timer(self._flush_interval, self._timer_flush)
        self._timer.daemon = True
        self._timer.start()

    def _timer_flush(self) -> None:
        """Flush triggered by timer."""
        self.flush()
        self._start_flush_timer()

    def emit(self, record: logging.LogRecord) -> None:
        """Buffer a log record."""
        try:
            exc_text = None
            if record.exc_info:
                exc_text = self.formatException(record.exc_info)

            log_entry = {
                "timestamp": datetime.fromtimestamp(record.created, tz=UTC),
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record) if self.formatter else record.getMessage(),
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno,
                "thread": record.threadName,
                "exception": exc_text,
            }

            with self._lock:
                self._buffer.append(log_entry)
                if len(self._buffer) >= self._buffer_size:
                    self._flush_buffer()
        except Exception:  # noqa: BLE001
            self.handleError(record)

    def _flush_buffer(self) -> None:
        """Write buffered records to Delta table (must hold lock)."""
        if not self._buffer:
            return

        try:
            df = self._spark.createDataFrame(self._buffer, schema=_LOG_SCHEMA)
            if not self._table_exists:
                # Create schema and table on first write
                parts = self._table_name.rsplit(".", 1)
                if len(parts) == 2:
                    self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {parts[0]}")
                df.write.format("delta").mode("overwrite").saveAsTable(self._table_name)
                self._table_exists = True
            else:
                df.write.format("delta").mode("append").saveAsTable(self._table_name)
            self._buffer.clear()
        except Exception as e:  # noqa: BLE001
            print(f"Failed to write logs to Delta: {e}")  # noqa: T201

    def flush(self) -> None:
        """Flush all buffered records."""
        with self._lock:
            self._flush_buffer()

    def close(self) -> None:
        """Close handler and flush remaining records."""
        if self._timer:
            self._timer.cancel()
        self.flush()
        super().close()
