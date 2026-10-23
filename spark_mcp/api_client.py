"""Public client facade for spark_mcp."""

from spark_mcp.api.api_client_spark import SparkApi, SparkApiError

__version__ = "0.4.1"

__all__ = ["Api", "SparkApiError"]


class Api(SparkApi):
    """Authenticated-when-possible Spark Connect client."""

    pass
