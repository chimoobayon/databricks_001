"""
base_classes.py
---------------
Clases abstractas que definen los contratos del pipeline ETL.

Principios SOLID aplicados:
- O (Open/Closed): nuevas fuentes o capas se crean extendiendo estas clases,
  sin modificar el código existente.
- L (Liskov Substitution): cualquier subclase es intercambiable con su base.
- I (Interface Segregation): IWriter solo expone write(); BaseExtractor y
  BaseTransformer no mezclan responsabilidades.
"""

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# I — Interface Segregation: interfaz mínima de escritura
# ---------------------------------------------------------------------------

class IWriter(ABC):
    """Contrato de escritura. Solo sabe persistir un DataFrame."""

    @abstractmethod
    def write(self, df: DataFrame, table_name: str) -> None:
        """Persiste *df* con el nombre de tabla indicado."""


# ---------------------------------------------------------------------------
# O / L — Extractor base (template method)
# ---------------------------------------------------------------------------

class BaseExtractor(ABC):
    """
    Extractor genérico.
    Las subclases implementan extract() y save(); run() orquesta el flujo.
    """

    def __init__(self, spark: SparkSession, writer: IWriter) -> None:
        # D — Dependency Inversion: recibe las dependencias, no las crea.
        self.spark = spark
        self.writer = writer

    @abstractmethod
    def extract(self) -> DataFrame:
        """Obtiene los datos de la fuente y los devuelve como DataFrame."""

    @abstractmethod
    def save(self, df: DataFrame) -> None:
        """Persiste el DataFrame usando self.writer."""

    def run(self) -> None:
        """Template method: extrae y guarda."""
        logger.info(f"[{self.__class__.__name__}] Iniciando extracción...")
        df = self.extract()
        logger.info(f"[{self.__class__.__name__}] Extracción completada: {df.count()} filas.")
        self.save(df)
        logger.info(f"[{self.__class__.__name__}] Guardado completado.")


# ---------------------------------------------------------------------------
# O / L — Transformer base (template method)
# ---------------------------------------------------------------------------

class BaseTransformer(ABC):
    """
    Transformer genérico.
    Las subclases implementan transform() y save(); run() orquesta el flujo.
    """

    def __init__(self, spark: SparkSession, writer: IWriter) -> None:
        self.spark = spark
        self.writer = writer

    @abstractmethod
    def transform(self) -> DataFrame:
        """Lee los datos de entrada, aplica las transformaciones y los devuelve."""

    @abstractmethod
    def save(self, df: DataFrame) -> None:
        """Persiste el DataFrame usando self.writer."""

    def run(self) -> None:
        """Template method: transforma y guarda."""
        logger.info(f"[{self.__class__.__name__}] Iniciando transformación...")
        df = self.transform()
        logger.info(f"[{self.__class__.__name__}] Transformación completada: {df.count()} filas.")
        self.save(df)
        logger.info(f"[{self.__class__.__name__}] Guardado completado.")
