# Proyecto Final — TLC Trip Record Data

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
.venv\Scripts\activate
```

Instalar dependencias:

```bash
pip install -r requirements.txt
```

## Crear estructura del proyecto

Ejecutar desde la carpeta raíz del proyecto:

```bash
python create_tlc_project_structure.py --root .
```

Para sobrescribir archivos base como README, `.gitignore` o documentación:

```bash
python create_tlc_project_structure.py --root . --force
```

## Ejecución sugerida

Ejecutar ingesta Nivel 2 y 3:

```bash
python src/ingestion/tlc_ingestion_nivel_2_3.py --years 2023 2024 2025 2026 --max-files none
```
Dia de la presentación
```bash
python tlc_ingestion_nivel_2_3.py --years 2026 --max-files none
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
- file_name
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
