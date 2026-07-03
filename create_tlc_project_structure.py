#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Crea la estructura base del proyecto final TLC Trip Record Data.

Uso:
    python create_tlc_project_structure.py
    python create_tlc_project_structure.py --root .
    python create_tlc_project_structure.py --root "C:/proyectos/PROYECTO-FINAL" --force

El script:
- Crea carpetas para arquitectura medallion/lakehouse.
- Agrega .gitkeep en carpetas vacías.
- Crea .gitignore para evitar subir datasets grandes a GitHub.
- Crea archivos base de documentación y configuración si no existen.
"""

from __future__ import annotations

import argparse
from pathlib import Path


README_TEMPLATE = """# Proyecto Final — TLC Trip Record Data

Este repositorio contiene el desarrollo del caso **TLC Trip Record Data** para la toma de decisiones mediante análisis descriptivo, diagnóstico y predictivo.

La solución se organiza bajo una arquitectura tipo **Medallion / Lakehouse**, con control de auditoría transversal y procesamiento en **PySpark**.

## Fuente de datos

Fuente oficial:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Datasets considerados:

- Yellow Taxi Trip Records
- Green Taxi Trip Records
- For-Hire Vehicle Trip Records
- High Volume For-Hire Vehicle Trip Records
- Taxi Zone Lookup Table

Años requeridos:

- 2023
- 2024
- 2025
- 2026, descargado automáticamente el día de ejecución según disponibilidad publicada.

## Objetivo del proyecto

Construir un flujo de datos completo que permita:

- Descargar datos oficiales del TLC.
- Ingerir archivos mensuales de forma automática.
- Registrar auditoría de ejecución.
- Organizar los datos en capas Raw, Bronze, Silver y Gold.
- Preparar datasets analíticos para dashboards.
- Preparar resultados para modelos de series de tiempo, segmentación y clasificación.

## Arquitectura general

### Nivel 1 — Fuente de datos

Contiene la fuente oficial NYC TLC y los archivos Parquet publicados mensualmente.

### Nivel 2 — Capa de ingesta

Responsable de:

- Construcción del catálogo de archivos esperados.
- Descarga automática e idempotente.
- Comparación contra histórico descargado.
- Registro de metadata de ingesta.
- Validación de existencia, tamaño, extensión, formato Parquet y legibilidad.
- Generación de manifest y logs de auditoría.

### Nivel 3 — Bronze Layer

Repositorio de datos crudos controlados.

Características:

- Archivos Parquet originales.
- Sin transformaciones analíticas.
- Metadata externa asociada:
  - fecha_ingesta
  - hora_ingesta
  - nombre_archivo
  - anio
  - mes
  - tipo_dataset
  - hash_archivo
  - pipeline_id
  - source_url
  - tamaño_archivo

### Nivel 4 — Silver Layer

Capa de limpieza y estandarización.

Procesos esperados:

- Limpieza de registros corruptos o vacíos.
- Validación de fechas, zonas, montos y distancias.
- Conversión de tipos.
- Normalización de nombres de columnas.
- Enriquecimiento temporal.
- Integración con Taxi Zone Lookup.
- Auditoría Silver.

### Nivel 5 — Gold Layer

Capa analítica optimizada para Power BI y modelos.

Datasets esperados:

- Viajes diarios.
- Revenue mensual.
- Viajes por borough.
- Horas pico.
- Análisis de pagos.
- Dataset de forecasting.
- Dataset de segmentación.
- Dataset de clasificación.

### Nivel 6 — Machine Learning Layer

Modelos esperados:

- Series de tiempo: predicción de viajes o ingresos.
- Segmentación: clustering de zonas o patrones de viaje.
- Clasificación: categorización de viajes según demanda, rentabilidad o duración.

### Nivel 7 — Dashboard Layer

Dashboards esperados:

- 3 dashboards descriptivos.
- 3 dashboards diagnósticos.
- 3 dashboards predictivos.
- 1 dashboard de auditoría del flujo de datos.

## Estructura del repositorio

```text
PROYECTO-FINAL/
│
├── config/
│   └── pipeline_config.yaml
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── lookup/
│
├── notebooks/
│   ├── ingestion/
│   ├── silver/
│   ├── gold/
│   ├── models/
│   └── dashboards/
│
├── src/
│   ├── ingestion/
│   ├── silver/
│   ├── gold/
│   ├── models/
│   ├── audit/
│   └── utils/
│
├── audit/
│   ├── ingestion/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── dashboards/
│
├── logs/
│
├── dashboards/
│   └── powerbi/
│
├── docs/
│
├── tests/
│
├── requirements.txt
├── .gitignore
└── README.md
```

## Instalación

Crear entorno virtual:

```bash
python -m venv .venv
```

Activar entorno en Windows:

```bash
.venv\\Scripts\\activate
```

Instalar dependencias:

```bash
pip install -r requirements.txt
```

## Ejecución sugerida

Crear estructura del proyecto:

```bash
python create_tlc_project_structure.py --root .
```

Ejecutar ingesta Nivel 2 y 3:

```bash
python src/ingestion/tlc_ingestion_nivel_2_3.py
```

Abrir notebooks:

```bash
jupyter lab
```

## Convención de capas

### Raw

Datos descargados desde la fuente oficial.

No se modifican.

### Bronze

Datos crudos controlados y auditados.

Se agregan metadatos técnicos, manifest y validaciones.

### Silver

Datos limpios, tipados, normalizados y enriquecidos.

### Gold

Datos agregados y optimizados para dashboards y modelos.

## Auditoría

El flujo de auditoría registra:

- pipeline_id
- pipeline_name
- pipeline_version
- started_at
- finished_at
- source_url
- local_path
- status
- error_message
- size_bytes
- sha256
- records_processed
- execution_time

Los archivos de auditoría se almacenan en:

```text
audit/
logs/
data/bronze/tlc_trip_records/_metadata/
```

## Política de GitHub

No se deben subir archivos pesados de datos al repositorio.

Se ignoran:

- data/raw/**
- data/bronze/**
- data/silver/**
- data/gold/**
- logs/**
- archivos temporales
- outputs locales

Se conservan las carpetas mediante archivos `.gitkeep`.

## Requisitos técnicos

- Python 3.11 o 3.12 recomendado.
- Java JDK 17 para PySpark.
- PySpark.
- pandas.
- pyarrow.
- rich.
- tqdm.

## Estado esperado del proyecto

El repositorio debe permitir demostrar:

- Arquitectura definida.
- Ingesta reproducible.
- Uso de PySpark.
- Control de auditoría.
- Separación por capas.
- Preparación para análisis descriptivo, diagnóstico y predictivo.
"""


GITIGNORE_TEMPLATE = """# Entorno Python
.venv/
venv/
env/
__pycache__/
*.py[cod]
.ipynb_checkpoints/
.pytest_cache/

# Variables locales
.env
.env.local

# Datos grandes
data/raw/**
data/bronze/**
data/silver/**
data/gold/**
data/landing/**
data/staging/**

# Mantener estructura vacía
!**/.gitkeep

# Logs y auditoría generados
logs/**
audit/**/*.jsonl
audit/**/*.parquet
audit/**/*.csv

# Mantener carpetas de auditoría
!logs/.gitkeep
!audit/**/.gitkeep

# Archivos temporales
*.tmp
*.part
*.bak
*.crc
_SUCCESS
_committed_*
_started_*

# Spark
spark-warehouse/
metastore_db/
derby.log

# Power BI / Office temporales
~$*
*.tmp

# Sistema operativo
.DS_Store
Thumbs.db
"""


REQUIREMENTS_TEMPLATE = """pyspark
pandas
pyarrow
requests
rich
tqdm
ipykernel
jupyterlab
"""


PIPELINE_CONFIG_TEMPLATE = """# Configuración base del pipeline TLC Trip Record Data

source:
  name: "NYC Taxi and Limousine Commission"
  url: "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
  cloudfront_base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"

ingestion:
  years:
    - 2023
    - 2024
    - 2025
    - 2026
  trip_types:
    - yellow
    - green
    - fhv
    - fhvhv
  closed_years:
    - 2023
    - 2024
    - 2025
  dynamic_years:
    - 2026
  overwrite_raw: false
  strict_completeness: true

paths:
  raw: "data/raw/tlc_trip_records"
  bronze: "data/bronze/tlc_trip_records"
  silver: "data/silver/tlc_trip_records"
  gold: "data/gold/tlc_trip_records"
  logs: "logs"
  audit: "audit"

audit:
  manifest_file: "logs/tlc_ingestion_manifest.jsonl"
  pipeline_name: "tlc_trip_record_data_pipeline"
  pipeline_version: "1.0.0"
"""


DOC_ARCHITECTURE_TEMPLATE = """# Arquitectura del Proyecto TLC

Este documento describe la arquitectura objetivo del proyecto.

## Nivel 1 — Fuente

Fuente oficial NYC TLC Trip Record Data.

## Nivel 2 — Ingesta

Capa encargada de descargar archivos, validar metadata y registrar auditoría.

## Nivel 3 — Bronze

Repositorio de Parquet crudos controlados y auditados.

## Nivel 4 — Silver

Limpieza, normalización, tipado y enriquecimiento.

## Nivel 5 — Gold

Datasets analíticos optimizados para BI y ML.

## Nivel 6 — Machine Learning

Series de tiempo, segmentación y clasificación.

## Nivel 7 — Dashboards

Power BI para análisis descriptivo, diagnóstico, predictivo y auditoría.
"""


DOC_AUDIT_TEMPLATE = """# Flujo de Auditoría

La auditoría registra el ciclo de vida del dato desde la descarga hasta las capas analíticas.

## Campos mínimos

- pipeline_id
- pipeline_name
- pipeline_version
- started_at
- finished_at
- file_name
- source_url
- local_path
- status
- error_message
- size_bytes
- sha256
- records_processed
- execution_time
"""


DOC_METADATA_TEMPLATE = """# Diccionario de Metadata

## Metadata de ingesta

| Campo | Descripción |
|---|---|
| fecha_ingesta | Fecha de carga del archivo |
| hora_ingesta | Hora de carga del archivo |
| nombre_archivo | Nombre físico del archivo |
| anio | Año del dataset |
| mes | Mes del dataset |
| tipo_dataset | yellow, green, fhv o fhvhv |
| hash_archivo | SHA256 del archivo |
| pipeline_id | Identificador único de ejecución |
| source_url | URL de origen |
"""


def write_text_if_missing(path: Path, content: str, force: bool = False) -> None:
    if path.exists() and not force:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def touch_gitkeep(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    gitkeep = directory / ".gitkeep"
    if not gitkeep.exists():
        gitkeep.write_text("", encoding="utf-8")


def build_structure(root: Path, force: bool = False) -> None:
    directories = [
        "config",

        "data/raw/tlc_trip_records",
        "data/bronze/tlc_trip_records/files",
        "data/bronze/tlc_trip_records/_metadata",
        "data/silver/tlc_trip_records",
        "data/gold/tlc_trip_records",
        "data/lookup/taxi_zone_lookup",
        "data/landing",
        "data/staging",

        "notebooks/ingestion",
        "notebooks/silver",
        "notebooks/gold",
        "notebooks/models",
        "notebooks/dashboards",

        "src/ingestion",
        "src/silver",
        "src/gold",
        "src/models/time_series",
        "src/models/segmentation",
        "src/models/classification",
        "src/audit",
        "src/utils",

        "audit/ingestion",
        "audit/bronze",
        "audit/silver",
        "audit/gold",
        "audit/dashboards",

        "logs",

        "dashboards/powerbi/descriptive",
        "dashboards/powerbi/diagnostic",
        "dashboards/powerbi/predictive",
        "dashboards/powerbi/audit",

        "reports/descriptive",
        "reports/diagnostic",
        "reports/predictive",
        "reports/audit",

        "docs",
        "tests",
    ]

    for directory in directories:
        touch_gitkeep(root / directory)

    # Paquetes Python
    package_dirs = [
        "src",
        "src/ingestion",
        "src/silver",
        "src/gold",
        "src/models",
        "src/models/time_series",
        "src/models/segmentation",
        "src/models/classification",
        "src/audit",
        "src/utils",
        "tests",
    ]

    for package_dir in package_dirs:
        init_file = root / package_dir / "__init__.py"
        write_text_if_missing(init_file, "", force=False)

    write_text_if_missing(root / "README.md", README_TEMPLATE, force=force)
    write_text_if_missing(root / ".gitignore", GITIGNORE_TEMPLATE, force=force)
    write_text_if_missing(root / "requirements.txt", REQUIREMENTS_TEMPLATE, force=force)
    write_text_if_missing(root / "config" / "pipeline_config.yaml", PIPELINE_CONFIG_TEMPLATE, force=force)
    write_text_if_missing(root / "docs" / "arquitectura.md", DOC_ARCHITECTURE_TEMPLATE, force=force)
    write_text_if_missing(root / "docs" / "flujo_auditoria.md", DOC_AUDIT_TEMPLATE, force=force)
    write_text_if_missing(root / "docs" / "diccionario_metadata.md", DOC_METADATA_TEMPLATE, force=force)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crea la estructura base del proyecto TLC Trip Record Data."
    )
    parser.add_argument(
        "--root",
        default=".",
        help="Directorio raíz del proyecto. Por defecto: carpeta actual.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Sobrescribe README, .gitignore, requirements y documentos base si ya existen.",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=True)

    build_structure(root=root, force=args.force)

    print(f"Estructura creada en: {root}")
    print("Listo para inicializar Git:")
    print("  git init")
    print("  git add .")
    print('  git commit -m "Estructura inicial del proyecto TLC"')


if __name__ == "__main__":
    main()
