"""
01_extract_movies_raw.py
------------------------
Job de extracción: películas italianas desde TMDB → tabla Delta raw.

Principios SOLID aplicados:
- S (Single Responsibility): MoviesExtractor solo orquesta la extracción de
  películas; TMDBClient se encarga de la API, DeltaWriter de la escritura.
- O (Open/Closed): extiende BaseExtractor sin modificar las clases base.
- L (Liskov Substitution): es intercambiable con cualquier otro BaseExtractor.
- D (Dependency Inversion): recibe TMDBClient y DeltaWriter (abstracciones)
  como dependencias inyectadas; main() es el único punto que toca dbutils.
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame, Row
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from base_classes import BaseExtractor, IWriter
from delta_writer import DeltaWriter
from tmdb_client import TMDBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

TABLE_NAME = "etl_cine.peliculas_italianas_raw"
NUM_PAGES = 10

PELICULAS_SCHEMA = StructType([
    StructField("pelicula_id",      StringType(),  False),
    StructField("titulo",           StringType(),  True),
    StructField("titulo_original",  StringType(),  True),
    StructField("fecha_estreno",    StringType(),  True),
    StructField("anio_estreno",     IntegerType(), True),
    StructField("popularidad",      DoubleType(),  True),
    StructField("valoracion_media", DoubleType(),  True),
    StructField("num_votos",        IntegerType(), True),
    StructField("idioma_original",  StringType(),  True),
    StructField("descripcion",      StringType(),  True),
    StructField("genero_ids",       StringType(),  True),
])


class MoviesExtractor(BaseExtractor):
    """
    Extrae películas italianas desde la API de TMDB y las guarda en la
    capa Raw como tabla Delta.
    """

    def __init__(self, spark, client: TMDBClient, writer: IWriter) -> None:
        super().__init__(spark, writer)
        self._client = client

    # ------------------------------------------------------------------
    # Implementación del contrato BaseExtractor
    # ------------------------------------------------------------------

    def extract(self) -> DataFrame:
        """Llama a TMDB, parsea los resultados y devuelve un DataFrame."""
        peliculas_raw = self._client.get_movies(num_pages=NUM_PAGES)
        filas = [self._parsear_pelicula(p) for p in peliculas_raw]
        df = self.spark.createDataFrame(filas, schema=PELICULAS_SCHEMA)
        logger.info(f"DataFrame creado con {df.count()} filas.")
        return df

    def save(self, df: DataFrame) -> None:
        """Persiste el DataFrame en la capa Raw."""
        self.writer.write(df, TABLE_NAME)

    # ------------------------------------------------------------------
    # Método privado de parseo
    # ------------------------------------------------------------------

    @staticmethod
    def _parsear_pelicula(p: dict) -> Row:
        """Convierte un dict de la API TMDB en una Row de Spark."""
        anio = int(p["release_date"][:4]) if p.get("release_date") else None
        return Row(
            pelicula_id      = str(p.get("id", "")),
            titulo           = p.get("title", ""),
            titulo_original  = p.get("original_title", ""),
            fecha_estreno    = p.get("release_date", ""),
            anio_estreno     = anio,
            popularidad      = float(p.get("popularity", 0.0)),
            valoracion_media = float(p.get("vote_average", 0.0)),
            num_votos        = int(p.get("vote_count", 0)),
            idioma_original  = p.get("original_language", ""),
            descripcion      = p.get("overview", ""),
            genero_ids       = ",".join(str(g) for g in p.get("genre_ids", [])),
        )


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main() -> None:
    # D — Dependency Inversion: único punto donde se leen credenciales y
    # se instancian las dependencias concretas antes de inyectarlas.
    api_key = dbutils.secrets.get(scope="tmdb", key="api_key")  # noqa: F821
    client  = TMDBClient(api_key)
    writer  = DeltaWriter(spark)                                  # noqa: F821
    MoviesExtractor(spark, client, writer).run()                  # noqa: F821


if __name__ == "__main__":
    main()
