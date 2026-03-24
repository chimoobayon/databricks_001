"""
delta_writer.py
---------------
Implementación concreta de IWriter para tablas Delta en Databricks.

Principios SOLID aplicados:
- S (Single Responsibility): solo escribe DataFrames como tablas Delta.
  No extrae datos, no transforma, no llama APIs.
- I (Interface Segregation): implementa únicamente el contrato IWriter,
  que expone un único método write().
- D (Dependency Inversion): recibe la sesión de Spark como dependencia;
  el código de negocio depende de IWriter (abstracción), no de esta clase.
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame, SparkSession

from base_classes import IWriter

logger = logging.getLogger(__name__)

DATABASE = "etl_cine"


class DeltaWriter(IWriter):
    """Persiste DataFrames como tablas Delta gestionadas por Unity Catalog."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._ensure_database()

    # ------------------------------------------------------------------
    # Implementación del contrato IWriter
    # ------------------------------------------------------------------

    def write(self, df: DataFrame, table_name: str) -> None:
        """
        Escribe *df* como tabla Delta en modo overwrite.

        Args:
            df:         DataFrame a persistir.
            table_name: Nombre completo de la tabla (p.ej. 'etl_cine.tabla').
        """
        logger.info(f"Escribiendo tabla '{table_name}' ({df.count()} filas)...")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        logger.info(f"Tabla '{table_name}' guardada correctamente.")

    # ------------------------------------------------------------------
    # Métodos privados de soporte
    # ------------------------------------------------------------------

    def _ensure_database(self) -> None:
        """Crea la base de datos si no existe."""
        self._spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
        logger.info(f"Base de datos '{DATABASE}' verificada.")
