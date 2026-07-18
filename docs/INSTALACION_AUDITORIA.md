# Instalación

## Archivos nuevos

Copiar en la raíz respetando rutas:

- `src/audit/__init__.py`
- `src/audit/audit_manager.py`
- `src/audit/command_runner.py`
- `src/audit/export_audit.py`
- `src/pipeline.py`
- `config/audit_pipeline.yaml`
- `tests/test_audit_manager.py`
- `requirements.txt`

## Scripts existentes

No modificar todavía:

- `src/ingestion/tlc_ingestion_nivel_2_3.py`
- `src/silver/01_profiling_inicial/profiling_inicial.py`
- `src/silver/02_detect_schema_changes/detect_schema_changes.py`
- `src/silver/03_generate_unified_schema/generate_unified_schema.py`
- `src/silver/04_limpieza/bronze_to_silver.py`
- `src/silver/05_analisis_de_calidad/quality_analysis.py`
- `src/silver/06_profiling_final/profiling_final.py`

Sus JSONL se mantienen en sus rutas actuales. El runner detecta los JSONL,
JSON y Parquet creados o modificados y los registra en `file_runs.parquet`.

## `.gitignore`

Añadir:

```gitignore
audit/runtime/
logs/pipeline/
```

## Comandos

```bash
pip install -r requirements.txt
pytest tests/test_audit_manager.py -v
python -m src.pipeline --years 2026
```
