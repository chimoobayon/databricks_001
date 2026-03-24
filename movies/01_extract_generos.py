"""
01_extract_generos.py
---------------------
Job de extracción: catálogo de géneros desde TMDB → tabla Delta raw.

Principios SOLID aplicados:
- S (Single Responsibility): GenresExtractor solo orquesta la extracción de
  géneros; la lógica HTTP está en TMDBClient, la de escritura en DeltaWriter.
- O (Open/Closed): extiende BaseExtractor sin modificar las clases base.
- L (Liskov Substitution): es intercambiable con MoviesExtractor u otro
  BaseExtractor en cualquier contexto que espere un extractor genérico.
- D (Dependency Inversion): recibe TMDBClient y DeltaWriter como dependencias
  inyectadas; main() es el único punto que toca dbutils.
"""

import logging
import os
import sys

try:
    _dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    import inspect as _inspect
    _dir = os.path.dirname(os.path.abspath(_inspect.getframeinfo(_inspect.currentframe()).filename))
sys.path.insert(0, _dir)

from pyspark.sql import DataFrame, Row
from pyspark.sql.types import StringType, StructField, StructType

from base_classes import BaseExtractor, IWriter
from delta_writer import DeltaWriter
from tmdb_client import TMDBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

TABLE_NAME = "etl_cine.generos_raw"

GENEROS_SCHEMA = StructType([
    StructField("genero_id",     StringType(), False),
    StructField("genero_nombre", StringType(), False),
])


class GenresExtractor(BaseExtractor):
    """
    Extrae el catálogo de géneros desde la API de TMDB y lo guarda en la
    capa Raw como tabla Delta.
    """

    def __init__(self, spark, client: TMDBClient, writer: IWriter) -> None:
        super().__init__(spark, writer)
        self._client = client

    # ------------------------------------------------------------------
    # Implementación del contrato BaseExtractor
    # ------------------------------------------------------------------

    def extract(self) -> DataFrame:
        """Llama a TMDB, parsea los géneros y devuelve un DataFrame."""
        generos_raw = self._client.get_genres()
        filas = [self._parsear_genero(g) for g in generos_raw]
        df = self.spark.createDataFrame(filas, schema=GENEROS_SCHEMA)
        logger.info(f"DataFrame de géneros creado con {df.count()} filas.")
        return df

    def save(self, df: DataFrame) -> None:
        """Persiste el DataFrame en la capa Raw."""
        self.writer.write(df, TABLE_NAME)

    # ------------------------------------------------------------------
    # Método privado de parseo
    # ------------------------------------------------------------------

    @staticmethod
    def _parsear_genero(g: dict) -> Row:
        """Convierte un dict de la API TMDB en una Row de Spark."""
        return Row(
            genero_id     = str(g["id"]),
            genero_nombre = g["name"],
        )


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main() -> None:
    api_key = dbutils.secrets.get(scope="tmdb", key="api_key")  # noqa: F821
    client  = TMDBClient(api_key)
    writer  = DeltaWriter(spark)                                  # noqa: F821
    GenresExtractor(spark, client, writer).run()                  # noqa: F821


if __name__ == "__main__":
    main()
