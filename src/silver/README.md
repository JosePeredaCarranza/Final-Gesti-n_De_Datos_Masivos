# PIPELINE EN VIVO
# 1. Primero, asegurarte de que los datos de 2026 están descargados en BRONZE
# (Esto lo hace el script de ingesta)

# 2. Ejecutar TODOS los scripts en orden para 2026
cd src/silver

# Script 1: Profiling de BRONZE
cd 01_profiling_inicial
python profiling_bronze.py --taxi all --years 2026

# Script 2: Detectar cambios de esquema
cd ../02_detect_schema_changes
python detect_schema_changes.py --taxi all --years 2026

# Script 3: Generar esquema unificado (solo si hay cambios)
cd ../03_generate_unified_schema
python generate_unified_schema.py --taxi all --force

# Script 4: BRONZE → SILVER (Limpieza)
cd ../04_limpieza
python bronze_to_silver.py --taxi all --years 2026

# Script 5: Análisis de calidad
cd ../05_analisis_de_calidad
python quality_analysis.py --taxi all --years 2026

# Script 6: Profiling final
cd ../06_profiling_silver
python profiling_final.py --taxi all --years 2026

# Desde la raíz del proyecto
python src/silver/01_profiling_inicial/profiling_bronze.py --taxi all --years 2026
python src/silver/02_detect_schema_changes/detect_schema_changes.py --taxi all --years 2026
python src/silver/03_generate_unified_schema/generate_unified_schema.py --taxi all --force
python src/silver/04_limpieza/bronze_to_silver.py --taxi all --years 2026
python src/silver/05_analisis_de_calidad/quality_analysis.py --taxi all --years 2026
python src/silver/06_profiling_silver/profiling_final.py --taxi all --years 2026