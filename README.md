# Proyecto Final — TLC Trip Record Data

Pipeline reproducible para la ingesta, control, limpieza y auditoría de los registros de viajes de la **New York City Taxi and Limousine Commission (NYC TLC)**. El proyecto aplica una arquitectura **Medallion / Lakehouse** y PySpark para preparar datos para Power BI.

## Objetivos

- Descargar los datasets oficiales de TLC de forma idempotente.
- Preservar datos originales en Raw y una copia auditada en Bronze.
- Estandarizar, validar y enriquecer los datos en Silver.
- Registrar ejecuciones, etapas, reglas de calidad y artefactos.
- Publicar vistas Parquet de auditoría para su posterior visualización.
- Preparar un modelo estrella Parquet para análisis de negocio.

## Fuente de datos

Fuente oficial: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

Datasets considerados:

- Yellow Taxi Trip Records
- Green Taxi Trip Records
- For-Hire Vehicle (FHV) Trip Records
- High Volume For-Hire Vehicle (FHVHV) Trip Records
- Taxi Zone Lookup Table

Se procesan los años 2023, 2024 y 2025 como años cerrados. Para 2026, el pipeline usa únicamente los archivos publicados al momento de la ejecución.

## Arquitectura

```text
NYC TLC (Parquet/CSV)
        |
        v
Ingesta -> Raw -> Bronze -> Silver -> Gold (notebook) -> Power BI de negocio
                              |
                              v
                    Auditoría transversal -> Power BI de auditoría
```

La secuencia Silver es:

```text
Profiling inicial
  -> detección de cambios de esquema
  -> esquema unificado
  -> limpieza y enriquecimiento
  -> análisis de calidad
  -> profiling final
```

## Estructura

```text
.
├── config/
│   ├── pipeline_config.yaml
│   └── audit_pipeline.yaml
├── data/                       # Datos y resultados generados; ignorados por Git
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── logs/
├── src/
│   ├── ingestion/tlc_ingestion_nivel_2_3.py
│   ├── silver/
│   │   ├── 01_profiling_inicial/
│   │   ├── 02_detect_schema_changes/
│   │   ├── 03_generate_unified_schema/
│   │   ├── 04_limpieza/
│   │   ├── 05_analisis_de_calidad/
│   │   └── 06_profiling_final/
│   ├── audit/
│   └── pipeline.py
├── notebooks/
│   └── gold/modelo_estrella_parquet_powerbi.ipynb
├── dashboards/powerbi/
├── audit/runtime/              # Generado; ignorado por Git
├── tests/
├── requirements.txt
└── requirements-audit.txt
```

## Requisitos

- Python 3.11 o 3.12
- Java JDK 17 para PySpark
- Power BI Desktop para los dashboards
- Espacio libre suficiente: varios años y tipos de taxi pueden ocupar decenas de GB

## Instalación

Desde la raíz del repositorio:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-audit.txt
```

`requirements.txt` contiene las dependencias de procesamiento. `requirements-audit.txt` agrega PyYAML y pytest, requeridos por el orquestador auditado y las pruebas.

Compruebe el entorno:

```powershell
python -m src.pipeline --help
python src\ingestion\tlc_ingestion_nivel_2_3.py --help
pytest tests -v
```

## Ejecución

### Ingesta, Raw y Bronze

Prueba acotada:

```powershell
python src\ingestion\tlc_ingestion_nivel_2_3.py `
  --years 2026 `
  --trip-types yellow green `
  --max-files 4 `
  --workspace-dir data
```

Ejecución completa:

```powershell
python src\ingestion\tlc_ingestion_nivel_2_3.py `
  --years 2023 2024 2025 2026 `
  --trip-types all `
  --max-files none `
  --workspace-dir data
```

Salidas principales:

```text
data/raw/
data/bronze/files/trip_data/
data/bronze/files/lookup/
data/bronze/_metadata/
data/logs/tlc_ingestion_manifest.jsonl
```

La ingesta valida extensión, firma Parquet, metadatos, hash SHA-256 y lectura mediante PySpark.

### Pipeline auditado

Ejecute el orquestador como módulo:

```powershell
python -m src.pipeline --years 2026 --taxi all --force-quality
```

No use `python src/pipeline.py`; al ejecutarlo como módulo se resuelven correctamente los imports de `src`.

Opciones útiles:

```powershell
# Reprocesar Silver usando Bronze existente
python -m src.pipeline --years 2026 --taxi all --skip-ingestion

# Ejecutar un intervalo de etapas
python -m src.pipeline --years 2026 --taxi yellow `
  --from-stage SILVER_04_BRONZE_TO_SILVER `
  --to-stage SILVER_06_PROFILING_FINAL
```

### Ejecución manual de Silver

```powershell
python src\silver\01_profiling_inicial\profiling_inicial.py --taxi all --years 2026
python src\silver\02_detect_schema_changes\detect_schema_changes.py --taxi all --years 2023 2024 2025 2026
python src\silver\03_generate_unified_schema\generate_unified_schema.py --taxi all --force
python src\silver\04_limpieza\bronze_to_silver.py --taxi all --years 2026 --force
python src\silver\05_analisis_de_calidad\quality_analysis.py --taxi all --years 2026 --force
python src\silver\06_profiling_final\profiling_final.py --taxi all --years 2026 --force
```

## Auditoría

La auditoría registra el estado del pipeline, sus etapas, reglas de calidad y artefactos. Los resultados se escriben en `audit/runtime/` y los logs de comandos en `logs/pipeline/`.

Genere las vistas para Power BI:

```powershell
python -m src.audit.dashboard_views
Get-ChildItem .\audit\runtime\views\latest_*.parquet |
  Select-Object Name, Length, LastWriteTime
```

Las vistas principales son:

```text
audit/runtime/views/latest_pipeline_run.parquet
audit/runtime/views/latest_stage_runs.parquet
audit/runtime/views/latest_quality_results.parquet
audit/runtime/views/latest_file_runs.parquet
```

Antes de abrir Power BI, verifique que las vistas de calidad y archivos no estén vacías.

## Gold y análisis de negocio

El notebook `notebooks/gold/modelo_estrella_parquet_powerbi.ipynb` construye el modelo estrella Parquet para Power BI. Complete primero la capa Silver y abra Jupyter Lab:

```powershell
jupyter lab
```

Los directorios de modelos de series de tiempo, segmentación y clasificación están reservados para su implementación. Cada modelo futuro debe incluir datos de entrada, entrenamiento reproducible, métricas, artefacto persistido y documentación de interpretación.

## Pruebas

```powershell
pytest tests -v
```

Las pruebas actuales cubren la persistencia y exportación de auditoría, reglas de calidad y la estructura del notebook Gold. Se recomienda añadir una prueba de integración con una muestra pequeña de Parquet TLC para validar el flujo completo de ingesta a Silver.

## Criterios de demostración

1. Una ejecución exitosa y una fallida quedan registradas en auditoría.
2. Las vistas `latest_*` reflejan la última ejecución.
3. El dashboard muestra etapas y reglas de calidad fallidas, incluyendo su mensaje.
4. Los artefactos generados se pueden relacionar con su etapa.
5. Tras una nueva ejecución y actualizar Power BI, los datos cambian sin reimportar archivos.

## Notas operativas

- Los datos, logs y resultados generados no se versionan para evitar subir archivos pesados.
- Los meses futuros o aún no publicados por NYC TLC no son errores del pipeline.
- Para una ejecución completa, planifique memoria, almacenamiento y tiempo suficientes.
