from functools import wraps

from .unity_catalog_iomanager import DatabricksUnityCatalogInputManager


class DebugDatabricksUnityCatalogInputManager(DatabricksUnityCatalogInputManager):
    """Overwrites the UnityCatalogInputManager such that it adds LIMIT 10 to the original query"""

    @wraps(DatabricksUnityCatalogInputManager.form_query)
    def form_query(  # type: ignore
        self,
        catalog: str,
        schema: str,
        table: str,
        columns: list[str],
        predicate: str,
        partition_predicate: str | None,
    ) -> str:
        """Adds a LIMIT to the original query"""
        return (
            super().form_query(catalog, schema, table, columns, predicate, partition_predicate)
            + " LIMIT 10000"
        )  # type: ignore
