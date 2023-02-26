from .version import __version__

from .resources import TrinoConnection, trino_resource

__all__ = [
    "trino_resource",
    "TrinoConnection"
]