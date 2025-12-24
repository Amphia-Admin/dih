"""Runner orchestrator for transformation execution."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from src.dih.core.reader_registry import ReaderRegistry
from src.dih.core.writer_registry import WriterRegistry
from src.dih.utils.loader import DynamicLoader

if TYPE_CHECKING:
    from loadcore import PipelineConfig
    from src.dih.core.pipeline import Pipeline

logger = logging.getLogger(__name__)


class Runner:
    """Orchestrates transformation execution with automatic I/O.

    The Runner manages the complete ETL lifecycle:
    1. Apply Spark configuration overrides
    2. Initialize pipeline and registries
    3. Extract input data via registered readers
    4. Execute pipeline.process()
    5. Load output data via registered writers

    Examples
    --------
        from loadcore import Environment
        from dih import Runner

        env = Environment("./env.config.yaml")
        config = env.for_pipeline()

        runner = Runner(config=config, pipeline=MyPipeline)
        runner.run()
    """

    def __init__(
        self,
        config: PipelineConfig,
        pipeline: str | type[Pipeline],
    ) -> None:
        """Initialize the runner.

        Parameters
        ----------
        config : PipelineConfig
            Pipeline configuration from loadcore
        pipeline : str | type[Pipeline]
            Pipeline class or fully qualified class name
        """
        self._spark = config.spark
        self._catalog = config.catalog
        self._volumes = config.volumes
        self._metadata = config.metadata
        self._static_config = config.static_config
        self._spark_conf = config.spark_conf

        self._pipeline_ref = pipeline
        self._pipeline: Pipeline | None = None
        self._reader_registry: ReaderRegistry | None = None
        self._writer_registry: WriterRegistry | None = None

    def run(self) -> None:
        """Execute the complete pipeline lifecycle."""
        pipeline_start = time.perf_counter()
        logger.info("Starting pipeline execution")
        try:
            self._apply_spark_config()
            self._initialize_pipeline()
            self._extract_inputs()
            self._inject_metadata()
            self._execute_pipeline()
            self._load_outputs()

            total_duration = time.perf_counter() - pipeline_start
            logger.info(f"Pipeline execution completed successfully in {total_duration:.2f}s")
        except Exception:
            total_duration = time.perf_counter() - pipeline_start
            logger.exception(f"Pipeline execution failed after {total_duration:.2f}s")
            raise

    def _describe_df(self, df: DataFrame) -> str:
        """Return a concise description of a DataFrame."""
        row_count = df.count()
        col_count = len(df.columns)
        columns = ", ".join(df.columns[:5])
        if len(df.columns) > 5:
            columns += f", ... (+{len(df.columns) - 5} more)"
        return f"{row_count:,} rows x {col_count} cols [{columns}]"

    def _apply_spark_config(self) -> None:
        """Apply runtime Spark configuration overrides."""
        if not self._spark_conf:
            return

        for key, value in self._spark_conf.items():
            self._spark.conf.set(key, value)
            logger.debug(f"Set Spark config: {key}={value}")

    def _initialize_pipeline(self) -> None:
        """Instantiate pipeline and initialise registries."""
        logger.info("Initialising pipeline")

        # Load pipeline class
        pipeline_class = DynamicLoader.load_pipeline(self._pipeline_ref)
        logger.debug(f"Loaded pipeline class: {pipeline_class.__name__}")

        # Instantiate
        self._pipeline = pipeline_class()

        # Inject static config
        self._pipeline.static_config = self._static_config
        logger.debug(f"Injected static_config: {self._static_config}")

        # Initialize registries
        self._reader_registry = ReaderRegistry()
        self._writer_registry = WriterRegistry()
        logger.debug("Initialized reader and writer registries")

    def _extract_inputs(self) -> None:
        """Load input data via registered readers."""
        if self._pipeline is None or self._reader_registry is None:
            msg = "Pipeline or registry not initialized"
            raise RuntimeError(msg)

        extract_start = time.perf_counter()
        logger.info("Extracting input data")

        readers = self._reader_registry.get_readers(type(self._pipeline))
        logger.debug(f"Found {len(readers)} registered reader(s)")

        total_rows = 0
        for registered_reader in readers:
            alias = registered_reader.aliases[0]
            read_start = time.perf_counter()

            reader = registered_reader.read(catalog=self._catalog, volumes=self._volumes)
            df = reader.data
            self._pipeline.inputs[alias] = df

            read_duration = time.perf_counter() - read_start
            df_desc = self._describe_df(df)
            total_rows += df.count()
            logger.info(f"Read '{alias}': {df_desc} in {read_duration:.2f}s")

        extract_duration = time.perf_counter() - extract_start
        logger.info(f"Extraction complete: {len(self._pipeline.inputs)} input(s), {total_rows:,} total rows in {extract_duration:.2f}s")

    def _inject_metadata(self) -> None:
        """Inject metadata into pipeline."""
        if self._pipeline is None:
            msg = "Pipeline not initialized"
            raise RuntimeError(msg)

        self._pipeline.metadata = self._metadata
        logger.debug(f"Injected metadata: {self._metadata}")

    def _execute_pipeline(self) -> None:
        """Execute the pipeline's process method."""
        if self._pipeline is None:
            msg = "Pipeline not initialized"
            raise RuntimeError(msg)

        process_start = time.perf_counter()
        pipeline_name = type(self._pipeline).__name__
        logger.info(f"Executing pipeline: {pipeline_name}")

        self._pipeline.process()

        process_duration = time.perf_counter() - process_start
        output_count = len(self._pipeline.outputs)
        logger.info(f"Pipeline '{pipeline_name}' generated {output_count} output(s) in {process_duration:.2f}s")

    def _load_outputs(self) -> None:
        """Write output data via registered writers."""
        if self._pipeline is None or self._writer_registry is None:
            msg = "Pipeline or registry not initialized"
            raise RuntimeError(msg)

        load_start = time.perf_counter()
        logger.info("Loading output data")

        writers = self._writer_registry.get_writers(type(self._pipeline))
        logger.debug(f"Found {len(writers)} registered writer(s)")

        outputs = self._pipeline.outputs.results
        total_rows = 0
        written_count = 0

        for output_name, output_df in outputs.items():
            write_start = time.perf_counter()

            matched = False
            for registered_writer in writers:
                if output_name in registered_writer.aliases:
                    registered_writer.write(output_df, catalog=self._catalog, volumes=self._volumes)
                    matched = True
                    written_count += 1

                    write_duration = time.perf_counter() - write_start
                    df_desc = self._describe_df(output_df)
                    total_rows += output_df.count()
                    logger.info(f"Wrote '{output_name}': {df_desc} in {write_duration:.2f}s")
                    break

            if not matched:
                logger.warning(f"No writer registered for output: {output_name}")

        load_duration = time.perf_counter() - load_start
        logger.info(f"Load complete: {written_count} output(s), {total_rows:,} total rows in {load_duration:.2f}s")
