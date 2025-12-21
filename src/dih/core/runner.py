"""Runner orchestrator for transformation execution."""

import logging
from typing import TYPE_CHECKING, Any

from pyspark.sql import SparkSession

from src.dih.core.reader_registry import ReaderRegistry
from src.dih.core.writer_registry import WriterRegistry
from src.dih.utils.loader import DynamicLoader

if TYPE_CHECKING:
    from src.dih.core.pipeline import Pipeline

logger = logging.getLogger(__name__)


class Runner:
    """Orchestrates transformation execution with automatic I/O."""

    def __init__(
        self,
        run_config: dict[str, Any],
        pipeline: str | type[Pipeline],
    ) -> None:
        """
        Initialise runner.

        Parameters
        ----------
        run_config : dict[str, Any]
            Configuration dict with required 'root_path' and optional
            'metadata', 'static_config', 'spark_conf', 'app_name'
        pipeline : str | type[Pipeline]
            Fully qualified name (e.g., 'my.module.MyPipeline')
            or Pipeline class reference

        Raises
        ------
        ValueError
            If required config keys are missing
        """
        if "root_path" not in run_config:
            msg = "run_config must contain 'root_path' key"
            raise ValueError(msg)

        self._run_config = run_config
        self._pipeline_ref = pipeline
        self._pipeline: Pipeline | None = None
        self._reader_registry: ReaderRegistry | None = None
        self._writer_registry: WriterRegistry | None = None

    def run(self) -> None:
        """Execute complete pipeline: spark → init → extract → process → load."""
        logger.info("Starting pipeline execution")
        try:
            self._add_spark_confs_to_session()
            self._initialise_pipeline()
            self._extract_input_data()
            self._set_metadata()
            self._execute_pipeline()
            self._load_output_data()
            logger.info("Pipeline execution completed successfully")
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise

    def _add_spark_confs_to_session(self) -> None:
        """Create Spark session with Delta support."""
        # Apply runtime configs
        if "spark_conf" in self._run_config:
            for key, value in self._run_config["spark_conf"].items():
                SparkSession.getActiveSession().conf.set(key, value)
                logger.debug(f"Set Spark config: {key}={value}")

    def _initialise_pipeline(self) -> None:
        """Instantiate pipeline and initialise registries."""
        logger.info("Initialising pipeline")

        # Load pipeline class
        pipeline_class = DynamicLoader.load_pipeline(self._pipeline_ref)
        logger.debug(f"Loaded pipeline class: {pipeline_class.__name__}")

        # Instantiate
        self._pipeline = pipeline_class()

        # Inject static config
        static_config = self._run_config.get("static_config", {})
        self._pipeline.static_config = static_config
        logger.debug(f"Injected static_config: {static_config}")

        # initialise registries
        self._reader_registry = ReaderRegistry()
        self._writer_registry = WriterRegistry()
        logger.debug("initialised reader and writer registries")

    def _extract_input_data(self) -> None:
        """Load input data via registered readers."""
        if self._pipeline is None or self._reader_registry is None:
            msg = "Pipeline or registry not initialised"
            raise RuntimeError(msg)

        logger.info("Extracting input data")

        readers = self._reader_registry.get_readers(type(self._pipeline))
        logger.debug(f"Found {len(readers)} registered reader(s)")

        root_path = str(self._run_config["root_path"])
        for registered_reader in readers:
            alias = registered_reader.aliases[0]
            logger.debug(f"Reading input: {alias}")

            reader = registered_reader.read(root_path)
            self._pipeline.inputs[alias] = reader.data
            logger.debug(f"Loaded input '{alias}' with {reader.data.count()} rows")

        logger.info(f"Loaded {len(self._pipeline.inputs)} input(s)")

    def _set_metadata(self) -> None:
        """Inject metadata into pipeline."""
        if self._pipeline is None:
            msg = "Pipeline not initialised"
            raise RuntimeError(msg)

        metadata = self._run_config.get("metadata", {})
        self._pipeline.metadata = metadata
        logger.debug(f"Injected metadata: {metadata}")

    def _execute_pipeline(self) -> None:
        """Execute pipeline."""
        if self._pipeline is None:
            msg = "Pipeline not initialised"
            raise RuntimeError(msg)

        logger.info("Executing pipeline")
        self._pipeline.process()
        logger.info(f"Generated {len(self._pipeline.outputs)} output(s)")

    def _load_output_data(self) -> None:
        """Write output data via registered writers."""
        if self._pipeline is None or self._writer_registry is None:
            msg = "Pipeline or registry not initialised"
            raise RuntimeError(msg)

        logger.info("Loading output data")

        writers = self._writer_registry.get_writers(type(self._pipeline))
        logger.debug(f"Found {len(writers)} registered writer(s)")

        root_path = str(self._run_config["root_path"])
        outputs = self._pipeline.outputs.results

        for output_name, output_df in outputs.items():
            logger.debug(f"Writing output: {output_name}")

            # Find matching writer
            matched = False
            for registered_writer in writers:
                if output_name in registered_writer.aliases:
                    registered_writer.write(output_df, root_path)
                    matched = True
                    logger.debug(f"Wrote output '{output_name}' with {output_df.count()} rows")
                    break

            if not matched:
                logger.warning(f"No writer registered for output: {output_name}")

        logger.info("Output data loaded successfully")
