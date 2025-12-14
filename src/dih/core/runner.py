"""Runner orchestrator for transformation execution."""

import logging
from typing import TYPE_CHECKING, Any

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from dih.core.reader_registry import ReaderRegistry
from dih.core.writer_registry import WriterRegistry
from dih.utils.loader import DynamicLoader

if TYPE_CHECKING:
    from dih.core.transformation import Transformation

logger = logging.getLogger(__name__)


class Runner:
    """Orchestrates transformation execution with automatic I/O."""

    def __init__(
        self,
        run_config: dict[str, Any],
        transformation: str | type[Transformation],
    ) -> None:
        """
        Initialize runner.

        Parameters
        ----------
        run_config : dict[str, Any]
            Configuration dict with required 'root_path' and optional
            'metadata', 'static_config', 'spark_conf', 'app_name'
        transformation : str | type[Transformation]
            Fully qualified name (e.g., 'my.module.MyPipeline')
            or Transformation class reference

        Raises
        ------
        ValueError
            If required config keys are missing
        """
        if "root_path" not in run_config:
            msg = "run_config must contain 'root_path' key"
            raise ValueError(msg)

        self._run_config = run_config
        self._transformation_ref = transformation
        self._transformation: Transformation | None = None
        self._reader_registry: ReaderRegistry | None = None
        self._writer_registry: WriterRegistry | None = None

    def run(self) -> None:
        """Execute complete pipeline: spark → init → extract → process → load."""
        logger.info("Starting pipeline execution")
        try:
            self._create_spark_session()
            self._initialize_transformation()
            self._extract_input_data()
            self._set_metadata()
            self._execute_transformation()
            self._load_output_data()
            logger.info("Pipeline execution completed successfully")
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise

    def _create_spark_session(self) -> None:
        """Create Spark session with Delta support."""
        app_name = self._run_config.get("app_name", "DIH")
        logger.info(f"Creating Spark session: {app_name}")

        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.debug(f"Spark session created/retrieved: {spark.sparkContext.appName}")

        # Apply runtime configs
        if "spark_conf" in self._run_config:
            for key, value in self._run_config["spark_conf"].items():
                spark.conf.set(key, value)
                logger.debug(f"Set Spark config: {key}={value}")

    def _initialize_transformation(self) -> None:
        """Instantiate transformation and initialize registries."""
        logger.info("Initializing transformation")

        # Load transformation class
        transform_class = DynamicLoader.load_transformation(self._transformation_ref)
        logger.debug(f"Loaded transformation class: {transform_class.__name__}")

        # Instantiate
        self._transformation = transform_class()

        # Inject static config
        static_config = self._run_config.get("static_config", {})
        self._transformation.static_config = static_config
        logger.debug(f"Injected static_config: {static_config}")

        # Initialize registries
        self._reader_registry = ReaderRegistry()
        self._writer_registry = WriterRegistry()
        logger.debug("Initialized reader and writer registries")

    def _extract_input_data(self) -> None:
        """Load input data via registered readers."""
        if self._transformation is None or self._reader_registry is None:
            msg = "Transformation or registry not initialized"
            raise RuntimeError(msg)

        logger.info("Extracting input data")

        readers = self._reader_registry.get_readers(type(self._transformation))
        logger.debug(f"Found {len(readers)} registered reader(s)")

        root_path = str(self._run_config["root_path"])
        for registered_reader in readers:
            alias = registered_reader.aliases[0]
            logger.debug(f"Reading input: {alias}")

            reader = registered_reader.read(root_path)
            self._transformation.inputs[alias] = reader.data
            logger.debug(f"Loaded input '{alias}' with {reader.data.count()} rows")

        logger.info(f"Loaded {len(self._transformation.inputs)} input(s)")

    def _set_metadata(self) -> None:
        """Inject metadata into transformation."""
        if self._transformation is None:
            msg = "Transformation not initialized"
            raise RuntimeError(msg)

        metadata = self._run_config.get("metadata", {})
        self._transformation.metadata = metadata
        logger.debug(f"Injected metadata: {metadata}")

    def _execute_transformation(self) -> None:
        """Execute transformation process."""
        if self._transformation is None:
            msg = "Transformation not initialized"
            raise RuntimeError(msg)

        logger.info("Executing transformation")
        self._transformation.process()
        logger.info(f"Generated {len(self._transformation.outputs)} output(s)")

    def _load_output_data(self) -> None:
        """Write output data via registered writers."""
        if self._transformation is None or self._writer_registry is None:
            msg = "Transformation or registry not initialized"
            raise RuntimeError(msg)

        logger.info("Loading output data")

        writers = self._writer_registry.get_writers(type(self._transformation))
        logger.debug(f"Found {len(writers)} registered writer(s)")

        root_path = str(self._run_config["root_path"])
        outputs = self._transformation.outputs.results

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
