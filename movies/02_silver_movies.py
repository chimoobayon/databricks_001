"""
02_silver_movies.py
-------------------
Job de transformación Silver: limpieza, validación y resolución de géneros.

Principios SOLID aplicados:
- S (Single Responsibility): SilverTransformer solo transforma la capa Raw.
  Cada método privado tiene una única responsabilidad (_limpiar, _resolver_generos).
- O (Open/Closed): extiende BaseTransformer; añadir pasos de transformación
  no requiere modificar las clases base.
- L (Liskov Substitution): es intercambiable con cualquier otro BaseTransformer.
- D (Dependency Inversion): recibe spark y writer como dependencias inyectadas;
  no instancia nada concreto en su interior.
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    collect_list,
    concat_ws,
    explode,
    split,
    to_date,
    trim,
)
from pyspark.sql.functions import round as spark_round

from base_classes import BaseTransformer, IWriter
from delta_writer import DeltaWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

RAW_MOVIES_TABLE  = "etl_cine.peliculas_italianas_raw"
RAW_GENRES_TABLE  = "etl_cine.generos_raw"
SILVER_TABLE      = "etl_cine.peliculas_italianas_silver"
MIN_VOTOS         = 10


class SilverTransformer(BaseTransformer):
    """
    Lee las tablas Raw de películas y géneros, aplica limpieza y
    enriquecimiento, y guarda el resultado en la capa Silver.
    """

    def __init__(self, spark, writer: IWriter) -> None:
        super().__init__(spark, writer)

    # ------------------------------------------------------------------
    # Implementación del contrato BaseTransformer
    # ------------------------------------------------------------------

    def transform(self) -> DataFrame:
        """Carga, limpia y enriquece los datos Raw."""
        df_movies  = self.spark.table(RAW_MOVIES_TABLE)
        df_generos = self.spark.table(RAW_GENRES_TABLE)
        logger.info(f"Registros raw cargados: {df_movies.count()} películas, {df_generos.count()} géneros.")

        df_limpio = self._limpiar(df_movies)
        df_silver = self._resolver_generos(df_limpio, df_generos)
        return df_silver

    def save(self, df: DataFrame) -> None:
        """Persiste el DataFrame en la capa Silver."""
        self.writer.write(df, SILVER_TABLE)

    # ------------------------------------------------------------------
    # Métodos privados de transformación (S — cada uno hace una sola cosa)
    # ------------------------------------------------------------------

    def _limpiar(self, df: DataFrame) -> DataFrame:
        """
        Aplica filtros de calidad y normaliza los tipos de datos.
        - Elimina filas sin año de estreno, sin título o con menos de MIN_VOTOS.
        - Recorta espacios en campos de texto.
        - Redondea popularidad y valoración a 2 decimales.
        - Convierte fecha_estreno a tipo Date.
        """
        logger.info("Aplicando limpieza y validación...")
        df_limpio = (
            df
            .filter(col("anio_estreno").isNotNull())
            .filter(col("titulo").isNotNull())
            .filter(col("num_votos") >= MIN_VOTOS)
            .withColumn("titulo",           trim(col("titulo")))
            .withColumn("titulo_original",  trim(col("titulo_original")))
            .withColumn("popularidad",      spark_round(col("popularidad"), 2))
            .withColumn("valoracion_media", spark_round(col("valoracion_media"), 2))
            .withColumn("fecha_estreno",    to_date(col("fecha_estreno"), "yyyy-MM-dd"))
        )
        logger.info(f"Registros tras limpieza: {df_limpio.count()}")
        return df_limpio

    def _resolver_generos(self, df_movies: DataFrame, df_generos: DataFrame) -> DataFrame:
        """
        Sustituye los IDs de géneros por sus nombres reales.
        1. Explota la columna genero_ids (CSV) en filas individuales.
        2. Join con la tabla de géneros para obtener el nombre.
        3. Reagrupa los nombres en una cadena separada por comas por película.
        """
        logger.info("Resolviendo IDs de géneros a nombres...")
        df_exploded = (
            df_movies
            .withColumn("genero_id", explode(split(col("genero_ids"), ",")))
            .withColumn("genero_id", trim(col("genero_id")))
        )

        df_silver = (
            df_exploded
            .join(df_generos, on="genero_id", how="left")
            .groupBy(
                "pelicula_id", "titulo", "titulo_original", "fecha_estreno",
                "anio_estreno", "popularidad", "valoracion_media",
                "num_votos", "idioma_original", "descripcion",
            )
            .agg(concat_ws(", ", collect_list("genero_nombre")).alias("generos"))
        )

        logger.info(f"Registros en Silver tras resolución de géneros: {df_silver.count()}")
        return df_silver


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main() -> None:
    writer = DeltaWriter(spark)           # noqa: F821
    SilverTransformer(spark, writer).run()  # noqa: F821


if __name__ == "__main__":
    main()
