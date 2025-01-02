from collections.abc import Callable

import polars as pl
from dagster import (
    ConfigurableIOManager,
    InputContext,
)
from pydantic import ConfigDict

from .utils import read_uc_table


class DatabricksUnityCatalogInputManager(ConfigurableIOManager):
    """This IOManager can read data from a unity catalog table through a sql warehouse"""

    model_config = ConfigDict(
        extra="allow",
        frozen=False,
    )

    def __init__(self, token_generator: Callable[[], str], server_hostname: str, endpoint: str):
        """Initializes the UnityCatalogInputManager.

        Args:
            token_generator (Callable): Non-cached token generator callable object, to be called when loading input
            server_hostname (str): The hostname of the databricks environment
            endpoint (str): The Databricks SQL warehouse endpoint to connect
        """
        super().__init__()
        self.token_generator = token_generator
        self.server_hostname = server_hostname
        self.endpoint = endpoint

    @staticmethod
    def form_query(
        catalog: str,
        schema: str,
        table: str,
        columns: list[str] | None,
        predicate: str | None,
        partition_predicate: str | None,
    ) -> str:
        """Returns a SQL query that selects the required columns from the table"""
        if columns is None:
            columns = []
        columns_str = ", ".join([f"`{col}`" for col in columns]) if len(columns) else "*"

        if predicate is not None and partition_predicate is not None:
            final_predicate = f"{predicate} AND {partition_predicate}"
        elif predicate is not None and partition_predicate is None:
            final_predicate = predicate
        elif predicate is None and partition_predicate is not None:
            final_predicate = partition_predicate
        else:
            final_predicate = None

        if final_predicate is None:
            return f"SELECT {columns_str} FROM `{catalog}`.`{schema}`.`{table}`"
        else:
            return f"SELECT {columns_str} FROM `{catalog}`.`{schema}`.`{table}` WHERE {final_predicate}"

    def load_input(
        self,
        context: InputContext,
    ) -> pl.DataFrame:
        """Read data through a sql warehouse and return it as a polars dataframe"""
        if context.upstream_output is not None:
            upstream_metadata = context.upstream_output.metadata or {}
            catalog = upstream_metadata.get("catalog")
            schema = upstream_metadata.get("schema")
            table = upstream_metadata.get("table")
        else:
            raise Exception("upstream_output should not be empty.")

        metadata = context.metadata or {}
        metadata = upstream_metadata | metadata  # type: ignore
        columns = metadata.get("columns")
        predicate = metadata.get("predicate")
        partition_predicate = None
        if context.has_asset_partitions:
            partitions = context._asset_partitions_subset.subset  # type: ignore
            if len(partitions) > 1:
                partition_predicate = f"{metadata['partition_expr']} in {str(tuple(partitions))}"
            else:
                partition_predicate = f'{metadata["partition_expr"]} == "{list(partitions)[0]}"'
        if table is None:
            raise Exception("table not present in metadata")
        if schema is None:
            raise Exception("schema not present in metadata")
        if catalog is None:
            raise Exception("catalog not present in metadata")

        query = self.form_query(catalog, schema, table, columns, predicate, partition_predicate)

        return read_uc_table(query, self.token_generator, self.server_hostname, self.endpoint)

    def handle_output(self) -> None:  # type: ignore
        """We're not doing anything here as we only want to read data."""
        raise NotImplementedError(
            "Currently this IO Manager only support reading data, handling outputs is not supported",
        )
