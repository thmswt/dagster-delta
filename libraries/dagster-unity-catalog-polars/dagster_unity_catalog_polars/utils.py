import logging
from collections.abc import Callable

import polars as pl
from databricks import sql

logger = logging.getLogger()


def read_uc_table(
    query: str,
    token_generator: Callable[[], str],
    server_hostname: str,
    endpoint: str,
) -> pl.DataFrame:
    """Wrapper for databricks sql connector to read Unity Catalog data

    Args:
        query (str): SQL like query
        token_generator (Callable): A non-cached Callable object that returns a Databricks oauth token, function call will be executed inside this function.
        server_hostname (str): The hostname of the databricks environment
        endpoint (str): The Databricks SQL warehouse endpoint to connect

    Returns:
        pl.LazyFrame: dataframe


    Examples:
    --------
    >>> from dagster_unity_catalog_polars import read_uc_table
    >>> query = f"SELECT * FROM {catalog}.{schema}.{table}"
    read_uc_table(query, token_generator, server_hostname, endpoint)
    """
    conn = sql.connect(
        server_hostname=server_hostname,
        http_path=endpoint,
        access_token=token_generator(),
        use_cloud_fetch=True,
    )
    cursor = conn.cursor()
    logger.info(
        "Executing query: %s on dbw: %s with sqlwh: %s",
        query,
        server_hostname,
        endpoint,
    )
    cursor.execute(query)

    return pl.DataFrame(cursor.fetchall_arrow())
