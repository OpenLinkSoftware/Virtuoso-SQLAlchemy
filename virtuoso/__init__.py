from sqlalchemy.dialects import registry

registry.register("virtuoso.pyodbc", "virtuoso.alchemy", "VirtuosoDialect")

__version__ = "0.1.7"
