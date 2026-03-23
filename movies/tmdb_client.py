"""
tmdb_client.py
--------------
Cliente HTTP para la API de The Movie Database (TMDB).

Principios SOLID aplicados:
- S (Single Responsibility): esta clase solo sabe hablar con la API de TMDB.
  No conoce Spark, Delta ni ningún detalle de infraestructura.
- D (Dependency Inversion): recibe api_key y base_url como parámetros;
  no los lee directamente de dbutils ni de variables de entorno.
"""

import logging

import requests

logger = logging.getLogger(__name__)

BASE_URL_DEFAULT = "https://api.themoviedb.org/3"


class TMDBClient:
    """Encapsula todas las llamadas a la API REST de TMDB."""

    def __init__(self, api_key: str, base_url: str = BASE_URL_DEFAULT) -> None:
        self._api_key = api_key
        self._base_url = base_url

    # ------------------------------------------------------------------
    # Métodos públicos
    # ------------------------------------------------------------------

    def get_movies(self, num_pages: int = 10) -> list[dict]:
        """
        Obtiene películas italianas ordenadas por popularidad.

        Args:
            num_pages: Número de páginas a recuperar (20 películas/página).

        Returns:
            Lista de dicts con los datos crudos de cada película.

        Raises:
            RuntimeError: si la API devuelve un código de error en cualquier página.
        """
        peliculas: list[dict] = []

        for page in range(1, num_pages + 1):
            datos = self._get(
                "/discover/movie",
                params={
                    "with_original_language": "it",
                    "sort_by": "popularity.desc",
                    "page": page,
                    "include_adult": False,
                },
            )
            resultados = datos.get("results", [])
            peliculas.extend(resultados)
            logger.info(f"Página {page}/{num_pages} — {len(resultados)} películas obtenidas.")

        logger.info(f"Total películas extraídas: {len(peliculas)}")
        return peliculas

    def get_genres(self) -> list[dict]:
        """
        Obtiene el catálogo de géneros de películas en italiano.

        Returns:
            Lista de dicts con 'id' y 'name' de cada género.

        Raises:
            RuntimeError: si la API devuelve un código de error.
        """
        datos = self._get("/genre/movie/list", params={"language": "it"})
        generos = datos.get("genres", [])
        logger.info(f"Géneros obtenidos: {len(generos)}")
        return generos

    # ------------------------------------------------------------------
    # Método privado de transporte HTTP
    # ------------------------------------------------------------------

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """
        Realiza una petición GET autenticada y devuelve el JSON de respuesta.

        Args:
            endpoint: Ruta relativa del endpoint (p.ej. '/discover/movie').
            params:   Parámetros de query string adicionales.

        Returns:
            Diccionario con el JSON de respuesta.

        Raises:
            RuntimeError: si el código HTTP no es 200.
        """
        url = f"{self._base_url}{endpoint}"
        query = {"api_key": self._api_key, **(params or {})}

        response = requests.get(url, params=query)

        if response.status_code != 200:
            msg = f"Error en GET {endpoint}: HTTP {response.status_code}"
            logger.error(msg)
            raise RuntimeError(msg)

        return response.json()
