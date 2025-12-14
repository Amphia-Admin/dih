"""Processing result collection."""

import logging
from collections.abc import Iterator, Mapping

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class ProcessingResult(Mapping[str, DataFrame]):
    """Collection to store named DataFrames as processing results."""

    def __init__(self) -> None:
        self._results: dict[str, DataFrame] = {}

    def add(self, name: str, data: DataFrame) -> None:
        """Add a named result to the collection."""
        if name in self._results:
            logger.warning(f"Overwriting existing result with name: {name}")
        self._results[name] = data

    @property
    def results(self) -> dict[str, DataFrame]:
        """Return all results as a dictionary."""
        return self._results

    def __getitem__(self, item: str) -> DataFrame:
        """Get result by name."""
        try:
            return self._results[item]
        except KeyError:
            logger.error(f"No such item found: {item}")
            raise

    def __contains__(self, key: object) -> bool:
        """Check if result exists."""
        return key in self._results

    def __iter__(self) -> Iterator[str]:
        """Iterate over result names."""
        return iter(self._results)

    def __len__(self) -> int:
        """Return number of results."""
        return len(self._results)

    def __eq__(self, other: object) -> bool:
        """Compare with another ProcessingResult."""
        if isinstance(other, ProcessingResult):
            return self.results == other.results
        return False

    def __repr__(self) -> str:
        """Return string representation."""
        return f"ProcessingResult({list(self._results.keys())})"
