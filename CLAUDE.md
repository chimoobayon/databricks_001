# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Databricks ETL pipeline for Italian cinema analytics using a Medallion architecture (Raw → Silver → Gold). Data is sourced from the TMDB API and processed with PySpark/Delta Lake in Databricks.

## Deployment

Notebooks are deployed automatically to Databricks on push to `main` via GitHub Actions ([.github/workflows/deploy.yml](.github/workflows/deploy.yml)):

```bash
# Manual deploy using Databricks CLI
databricks workspace import_dir movies /Repos/david.bayon.alvarez@gmail.com/databricks_repo/movies --overwrite
```

The workflow requires two GitHub secrets: `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.

The TMDB API key is stored in Databricks Secrets under scope `tmdb`, key `api_key`.

## Architecture: Medallion Pipeline

All notebooks live in [movies/](movies/) and run sequentially:

| Notebook | Layer | Purpose |
|---|---|---|
| `01_01_extract_movies_raw.ipynb` | Raw | Fetches 200 Italian movies from TMDB API → `etl_cine.peliculas_italianas_raw` |
| `01_02_generos.ipynb` | Raw | Fetches genre reference data from TMDB → `etl_cine.generos_raw` |
| `02_silver_movies.ipynb` | Silver | Cleans, validates, resolves genre names → `etl_cine.peliculas_italianas_silver` |
| `03_gold_peliculas.ipynb` | Gold | Aggregates into ranking + year-over-year tables → `etl_cine.gold_ranking_popularidad`, `etl_cine.gold_evolucion_por_anio` |

The `ejemplo_esquema_local.ipynb` is a standalone reference/example notebook (employees schema) unrelated to the pipeline.

## Delta Tables (Unity Catalog)

All tables live in the `etl_cine` database, which is auto-created by the extraction notebooks if it doesn't exist. Tables use Delta format.

## Coding Standards

Every code change must follow the five SOLID principles:

| Principle | Rule |
|-----------|------|
| **S** — Single Responsibility | Each class/function has one reason to change. Separate API calls, transformations, and persistence into distinct classes. |
| **O** — Open/Closed | Extend behavior via subclasses (e.g. `BaseExtractor`, `BaseTransformer`); never modify existing base classes to add new features. |
| **L** — Liskov Substitution | Subclasses must be fully substitutable for their base class without altering correctness. |
| **I** — Interface Segregation | Keep interfaces small and focused (e.g. `IWriter` only exposes `write()`). No class should implement methods it doesn't use. |
| **D** — Dependency Inversion | High-level modules depend on abstractions, not concretions. Inject `spark`, `writer`, and `client` as constructor parameters; never instantiate them inside business logic. |

## Key Patterns

- **Secrets**: Retrieved via `dbutils.secrets.get(scope="...", key="...")`
- **Database creation**: `spark.sql("CREATE DATABASE IF NOT EXISTS etl_cine")`
- **Write mode**: `df.write.format("delta").mode("overwrite").saveAsTable("etl_cine.table_name")`
- **Genre handling**: `genero_ids` is stored as comma-separated string; Silver layer explodes, joins, and re-aggregates
- **Window functions**: Used in Gold layer for popularity ranking
