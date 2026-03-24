"""
03_gold_peliculas.py
--------------------
Job de transformación Gold: agregaciones para análisis y dashboarding.

Principios SOLID aplicados:
- S (Single Responsibility): GoldTransformer solo agrega datos de la capa
  Silver. Cada método privado crea una única tabla Gold.
- O (Open/Closed): extiende BaseTransformer; añadir nuevas tablas Gold implica
  añadir métodos privados, no modificar las clases base.
- L (Liskov Substitution): es intercambiable con cualquier otro BaseTransformer.
- D (Dependency Inversion): recibe spark y writer como dependencias inyectadas.
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

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count
from pyspark.sql.functions import rank as spark_rank
from pyspark.sql.functions import round as spark_round
from pyspark.sql.window import Window

from base_classes import BaseTransformer, IWriter
from delta_writer import DeltaWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

SILVER_TABLE         = "etl_cine.peliculas_italianas_silver"
GOLD_RANKING_TABLE   = "etl_cine.gold_ranking_popularidad"
GOLD_EVOLUCION_TABLE = "etl_cine.gold_evolucion_por_anio"


class GoldTransformer(BaseTransformer):
    """
    Lee la capa Silver y genera dos tablas Gold:
    - gold_ranking_popularidad: ranking de películas por popularidad.
    - gold_evolucion_por_anio:  métricas agregadas por año de estreno.
    """

    def __init__(self, spark, writer: IWriter) -> None:
        super().__init__(spark, writer)

    # ------------------------------------------------------------------
    # Implementación del contrato BaseTransformer
    # ------------------------------------------------------------------

    def transform(self) -> DataFrame:
        """
        Genera las tablas Gold y las persiste internamente.
        Devuelve df_ranking como DataFrame principal (usado por run() para
        loguear el conteo), aunque ambas tablas se guardan en save().
        """
        df_silver = self.spark.table(SILVER_TABLE)
        logger.info(f"Registros Silver cargados: {df_silver.count()}")

        self._df_ranking   = self._crear_ranking(df_silver)
        self._df_evolucion = self._crear_evolucion_anio(df_silver)

        return self._df_ranking

    def save(self, df: DataFrame) -> None:
        """Persiste las dos tablas Gold."""
        self.writer.write(self._df_ranking,   GOLD_RANKING_TABLE)
        self.writer.write(self._df_evolucion, GOLD_EVOLUCION_TABLE)

    # ------------------------------------------------------------------
    # Métodos privados de agregación (S — cada uno crea una única tabla)
    # ------------------------------------------------------------------

    def _crear_ranking(self, df: DataFrame) -> DataFrame:
        """
        Añade una columna de ranking por popularidad descendente y
        selecciona los campos relevantes para el dashboard.
        """
        logger.info("Creando tabla Gold: ranking de popularidad...")
        window_ranking = Window.orderBy(col("popularidad").desc())

        df_ranking = (
            df
            .withColumn("ranking", spark_rank().over(window_ranking))
            .select(
                "ranking",
                "titulo",
                "titulo_original",
                "anio_estreno",
                "generos",
                "popularidad",
                "valoracion_media",
                "num_votos",
            )
            .orderBy("ranking")
        )
        logger.info(f"Ranking creado con {df_ranking.count()} filas.")
        return df_ranking

    def _crear_evolucion_anio(self, df: DataFrame) -> DataFrame:
        """
        Agrega métricas por año de estreno: número de películas,
        valoración media, popularidad media y votos medios.
        """
        logger.info("Creando tabla Gold: evolución por año...")
        df_evolucion = (
            df
            .groupBy("anio_estreno")
            .agg(
                count("pelicula_id").alias("num_peliculas"),
                spark_round(avg("valoracion_media"), 2).alias("valoracion_media"),
                spark_round(avg("popularidad"), 2).alias("popularidad_media"),
                spark_round(avg("num_votos"), 0).alias("votos_medios"),
            )
            .orderBy("anio_estreno")
        )
        logger.info(f"Evolución por año creada con {df_evolucion.count()} filas.")
        return df_evolucion


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main() -> None:
    writer = DeltaWriter(spark)             # noqa: F821
    GoldTransformer(spark, writer).run()    # noqa: F821


if __name__ == "__main__":
    main()
