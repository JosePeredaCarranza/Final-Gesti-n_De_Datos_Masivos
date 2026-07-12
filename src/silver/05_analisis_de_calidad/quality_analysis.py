"""
ANÁLISIS DE CALIDAD - VERSIÓN PRODUCCIÓN
- Lee datos de SILVER cleaned (data/silver/cleaned/)
- Mide calidad con dimensiones de calidad de datos
- Genera reportes de calidad en JSON
- Logs en data/logs/quality_manifest.jsonl
- Incremental: Solo analiza años nuevos
"""
import os
import json
import argparse
import time
import sys
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# CONFIGURACIÓN
# ============================================================
ROOT_DIR = Path(__file__).parent.parent.parent.parent

if os.name == 'nt':
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = os.environ.get("PATH", "") + ";C:\\hadoop\\bin"

# Tipos de taxi activos (excluye FHVHV por defecto)
TAXI_TYPES_ACTIVE = ["fhv", "green", "yellow"]

# Umbrales de calidad
QUALITY_THRESHOLDS = {
    "completitud": 0.90,
    "precision": 0.85,
    "unicidad": 0.80,
    "validez": 0.90,
    "consistencia": 0.90
}

class QualityAnalyzer:
    def __init__(self, spark):
        self.spark = spark
        self.pipeline_id = f"quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.pipeline_name = "quality_analysis"
        self.pipeline_version = "1.0.0"
        self.start_time = datetime.now()
        
        # Rutas
        self.input_dir = ROOT_DIR / "data" / "silver" / "cleaned"
        self.base_dir = ROOT_DIR / "data" / "silver" / "quality"
        self.metadata_dir = self.base_dir / "_metadata"
        self.reports_dir = self.base_dir / "_quality_reports"
        self.log_dir = ROOT_DIR / "data" / "logs"
        self.log_path = self.log_dir / "quality_manifest.jsonl"
        self.metadata_path = self.metadata_dir / "metadata.jsonl"
        self.tracker_path = self.metadata_dir / "quality_tracker.json"
        
        # Crear directorios
        for path in [self.reports_dir, self.metadata_dir, self.log_dir]:
            path.mkdir(parents=True, exist_ok=True)
        
        # Inicializar tracker
        if not self.tracker_path.exists():
            with open(self.tracker_path, "w", encoding="utf-8") as f:
                json.dump({}, f, indent=2)
        
        # Estadísticas
        self.execution_stats = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.start_time.isoformat(),
            "root_dir": str(ROOT_DIR),
            "processed": [],
            "errors": []
        }
        
        self._log_info(f"Pipeline ID: {self.pipeline_id}")
    
    def _log_info(self, message):
        print(f"[INFO] {message}")
    
    def _log_error(self, message):
        print(f"[ERROR] {message}", file=sys.stderr)
    
    def _get_processed_years(self, taxi_type):
        if not self.tracker_path.exists():
            return []
        try:
            with open(self.tracker_path, "r", encoding="utf-8") as f:
                tracker = json.load(f)
            return tracker.get(taxi_type, [])
        except:
            return []
    
    def _save_processed_years(self, taxi_type, years):
        try:
            with open(self.tracker_path, "r", encoding="utf-8") as f:
                tracker = json.load(f)
        except:
            tracker = {}
        
        if taxi_type not in tracker:
            tracker[taxi_type] = []
        for year in years:
            if year not in tracker[taxi_type]:
                tracker[taxi_type].append(year)
        tracker[taxi_type] = sorted(tracker[taxi_type])
        
        with open(self.tracker_path, "w", encoding="utf-8") as f:
            json.dump(tracker, f, indent=2, default=str)
    
    def _get_available_years(self, taxi_type):
        base_path = self.input_dir / taxi_type
        if not base_path.exists():
            return []
        years = []
        for year_dir in base_path.glob("year=*"):
            try:
                year = int(year_dir.name.split("=")[1])
                years.append(year)
            except:
                continue
        return sorted(years)
    
    def _read_parquet_safe(self, input_path):
        """Lee archivos Parquet de forma segura (uno por uno)"""
        try:
            parquet_files = list(input_path.glob("*.parquet"))
            if not parquet_files:
                return None
            
            df = self.spark.read.parquet(str(parquet_files[0]))
            for file_path in parquet_files[1:]:
                try:
                    df_temp = self.spark.read.parquet(str(file_path))
                    df = df.union(df_temp)
                except:
                    continue
            return df
        except:
            return None
    
    def analyze_quality(self, df, taxi_type, year):
        """Analiza calidad con dimensiones de calidad de datos"""
        self._log_info(f"   Analizando calidad...")
        
        total_records = df.count()
        
        if total_records == 0:
            self._log_info(f"      No hay registros para analizar")
            return None
        
        quality_report = {
            "taxi_type": taxi_type,
            "year": year,
            "total_records": total_records,
            "dimensions": {},
            "overall_score": 0.0,
            "overall_passed": False,
            "issues": []
        }
        
        # ================================================================
        # DIMENSIÓN 1: COMPLETITUD (% de datos no nulos)
        # ================================================================
        self._log_info("      [Completitud] Midiendo % de datos no nulos...")
        total_cells = total_records * len(df.columns)
        null_cells = 0
        for col_name in df.columns:
            null_cells += df.filter(F.col(col_name).isNull()).count()
        
        completitud = 1 - (null_cells / total_cells) if total_cells > 0 else 0
        quality_report["dimensions"]["completitud"] = {
            "score": round(completitud, 4),
            "threshold": QUALITY_THRESHOLDS["completitud"],
            "passed": completitud >= QUALITY_THRESHOLDS["completitud"]
        }
        
        if not quality_report["dimensions"]["completitud"]["passed"]:
            quality_report["issues"].append(f"Completitud baja: {completitud:.2%}")
        
        self._log_info(f"         Completitud: {completitud:.2%} {'✅' if completitud >= 0.90 else '❌'}")
        
        # ================================================================
        # DIMENSIÓN 2: PRECISIÓN (valores en rangos esperados)
        # ================================================================
        self._log_info("      [Precisión] Midiendo valores en rangos esperados...")
        precision = 1.0
        
        if "total_amount" in df.columns:
            pct = df.filter(
                (F.col("total_amount") >= 0) & 
                (F.col("total_amount") <= 1000)
            ).count() / total_records
            precision = min(precision, pct)
        
        if "trip_distance" in df.columns:
            pct = df.filter(
                (F.col("trip_distance") >= 0.1) & 
                (F.col("trip_distance") <= 500)
            ).count() / total_records
            precision = min(precision, pct)
        
        quality_report["dimensions"]["precision"] = {
            "score": round(precision, 4),
            "threshold": QUALITY_THRESHOLDS["precision"],
            "passed": precision >= QUALITY_THRESHOLDS["precision"]
        }
        
        if not quality_report["dimensions"]["precision"]["passed"]:
            quality_report["issues"].append(f"Precisión baja: {precision:.2%}")
        
        self._log_info(f"         Precisión: {precision:.2%} {'✅' if precision >= 0.85 else '❌'}")
        
        # ================================================================
        # DIMENSIÓN 3: UNICIDAD (CORREGIDO - usando columnas clave)
        # ================================================================
        self._log_info("      [Unicidad] Midiendo % de registros únicos...")
        
        # Usar columnas clave para deduplicación (más eficiente)
        key_cols = ["pickup_datetime", "dropoff_datetime", "taxi_type", "PULocationID", "DOLocationID"]
        key_cols = [c for c in key_cols if c in df.columns]
        
        if key_cols:
            try:
                # Contar registros únicos usando columnas clave
                unique_count = df.select(key_cols).distinct().count()
                unicidad = unique_count / total_records if total_records > 0 else 0
                self._log_info(f"         Registros únicos: {unique_count:,}")
            except Exception as e:
                self._log_info(f"         Error calculando unicidad: {str(e)[:50]}")
                unicidad = 1.0
        else:
            unicidad = 1.0
        
        quality_report["dimensions"]["unicidad"] = {
            "score": round(unicidad, 4),
            "threshold": QUALITY_THRESHOLDS["unicidad"],
            "passed": unicidad >= QUALITY_THRESHOLDS["unicidad"]
        }
        
        if not quality_report["dimensions"]["unicidad"]["passed"]:
            quality_report["issues"].append(f"Unicidad baja: {unicidad:.2%}")
        
        self._log_info(f"         Unicidad: {unicidad:.2%} {'✅' if unicidad >= 0.80 else '❌'}")
        
        # ================================================================
        # DIMENSIÓN 4: VALIDEZ (% que cumple reglas)
        # ================================================================
        self._log_info("      [Validez] Midiendo % que cumple reglas...")
        validez = 1.0
        
        if "payment_type" in df.columns:
            pct = df.filter(
                (F.col("payment_type").between(1, 6)) | 
                F.col("payment_type").isNull()
            ).count() / total_records
            validez = min(validez, pct)
        
        if "pulocationid" in df.columns:
            pct = df.filter(
                (F.col("pulocationid").between(1, 263)) | 
                F.col("pulocationid").isNull()
            ).count() / total_records
            validez = min(validez, pct)
        
        quality_report["dimensions"]["validez"] = {
            "score": round(validez, 4),
            "threshold": QUALITY_THRESHOLDS["validez"],
            "passed": validez >= QUALITY_THRESHOLDS["validez"]
        }
        
        if not quality_report["dimensions"]["validez"]["passed"]:
            quality_report["issues"].append(f"Validez baja: {validez:.2%}")
        
        self._log_info(f"         Validez: {validez:.2%} {'✅' if validez >= 0.90 else '❌'}")
        
        # ================================================================
        # DIMENSIÓN 5: CONSISTENCIA (tipos de datos correctos)
        # ================================================================
        self._log_info("      [Consistencia] Verificando tipos de datos...")
        total_cols = len(df.dtypes)
        correct_types = 0
        for col_name, dtype in df.dtypes:
            if dtype in ['int', 'bigint', 'double', 'float', 'string', 'timestamp']:
                correct_types += 1
        
        consistencia = correct_types / total_cols if total_cols > 0 else 0
        quality_report["dimensions"]["consistencia"] = {
            "score": round(consistencia, 4),
            "threshold": QUALITY_THRESHOLDS["consistencia"],
            "passed": consistencia >= QUALITY_THRESHOLDS["consistencia"]
        }
        
        if not quality_report["dimensions"]["consistencia"]["passed"]:
            quality_report["issues"].append(f"Consistencia baja: {consistencia:.2%}")
        
        self._log_info(f"         Consistencia: {consistencia:.2%} {'✅' if consistencia >= 0.90 else '❌'}")
        
        # ================================================================
        # SCORE TOTAL PONDERADO
        # ================================================================
        weights = {
            "completitud": 0.30,
            "precision": 0.25,
            "unicidad": 0.15,
            "validez": 0.20,
            "consistencia": 0.10
        }
        
        overall_score = 0.0
        for dim, weight in weights.items():
            if dim in quality_report["dimensions"]:
                overall_score += quality_report["dimensions"][dim]["score"] * weight
        
        quality_report["overall_score"] = round(overall_score, 4)
        quality_report["overall_passed"] = overall_score >= 0.85
        
        self._log_info(f"      SCORE TOTAL DE CALIDAD: {overall_score:.2%}")
        self._log_info(f"      {'✅ APROBADO' if overall_score >= 0.85 else '❌ RECHAZADO'}")
        
        if quality_report["issues"]:
            self._log_info(f"      ⚠️ Issues detectados: {len(quality_report['issues'])}")
        
        return quality_report
    
    def process_year(self, taxi_type, year, force=False):
        """Procesa UN AÑO completo de UN tipo de taxi"""
        self._log_info(f"\nProcesando: {taxi_type.upper()} - {year}")
        start_time = time.time()
        
        processed_years = self._get_processed_years(taxi_type)
        if year in processed_years and not force:
            self._log_info(f"   {year} ya procesado. Usa --force para reprocesar.")
            return None
        
        # 1. Leer datos de SILVER cleaned
        input_path = self.input_dir / taxi_type / f"year={year}"
        if not input_path.exists():
            self._log_error(f"   No existe: {input_path}")
            return None
        
        self._log_info(f"   Leyendo datos de: {input_path}")
        df = self._read_parquet_safe(input_path)
        
        if df is None:
            self._log_error(f"   No se pudieron leer los datos")
            return None
        
        try:
            original_count = df.count()
        except:
            self._log_error(f"   Error al contar registros")
            return None
        
        self._log_info(f"   Registros: {original_count:,}")
        
        # 2. Analizar calidad
        quality_report = self.analyze_quality(df, taxi_type, year)
        
        if quality_report is None:
            self._log_error(f"   No se pudo generar reporte de calidad")
            return None
        
        # 3. Guardar reporte
        report_path = self.reports_dir / f"{taxi_type}_{year}_quality_report.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(quality_report, f, indent=2, default=str)
        
        elapsed = time.time() - start_time
        
        self._log_info(f"\n   {taxi_type.upper()} {year} COMPLETADO")
        self._log_info(f"   Reporte: {report_path}")
        self._log_info(f"   Score: {quality_report['overall_score']:.2%}")
        self._log_info(f"   Tiempo: {elapsed:.2f} segundos")
        
        # 4. Actualizar tracker
        self._save_processed_years(taxi_type, [year])
        
        # 5. Registrar estadísticas
        self.execution_stats["processed"].append({
            "taxi_type": taxi_type,
            "year": year,
            "records": original_count,
            "quality_score": quality_report["overall_score"],
            "passed": quality_report["overall_passed"],
            "execution_time": round(elapsed, 2)
        })
        
        self._log_audit(
            taxi_type=taxi_type,
            year=year,
            status="completed",
            message=f"Reporte de calidad generado: {report_path}",
            file_path=str(report_path),
            metrics={
                "records_analyzed": original_count,
                "quality_score": quality_report["overall_score"],
                "passed": quality_report["overall_passed"],
                "execution_time_seconds": round(elapsed, 2)
            }
        )
        
        return quality_report
    
    def process(self, taxi_types, years, force=False):
        """Procesa todos los tipos y años solicitados"""
        self._log_info(f"Procesando {len(taxi_types)} tipos de taxi")
        
        for taxi_type in taxi_types:
            self._log_info(f"\n{'='*50}")
            self._log_info(f"Categoria: {taxi_type.upper()}")
            self._log_info(f"{'='*50}")
            
            available_years = self._get_available_years(taxi_type)
            if not available_years:
                self._log_info(f"   No hay datos disponibles para {taxi_type}")
                continue
            
            years_to_process = [y for y in years if y in available_years]
            if not years_to_process:
                self._log_info(f"   No hay años solicitados disponibles")
                continue
            
            for year in years_to_process:
                try:
                    self.process_year(taxi_type, year, force)
                except Exception as e:
                    self._log_error(f"   Error en {taxi_type} {year}: {str(e)}")
                    self.execution_stats["errors"].append({
                        "taxi_type": taxi_type,
                        "year": year,
                        "error": str(e)
                    })
                    self._log_audit(
                        taxi_type=taxi_type,
                        year=year,
                        status="failed",
                        message="Error en análisis de calidad",
                        error=str(e)
                    )
        
        self.execution_stats["finished_at"] = datetime.now().isoformat()
        self.execution_stats["total_processed"] = len(self.execution_stats["processed"])
        self.execution_stats["total_errors"] = len(self.execution_stats["errors"])
        
        self._save_metadata()
        self._show_summary()
        
        return self.execution_stats
    
    def _log_audit(self, taxi_type, year, status, message, file_path=None, error=None, metrics=None):
        log_entry = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.start_time.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "trip_type": taxi_type,
            "year": year,
            "file_name": file_path.split("\\")[-1] if file_path else None,
            "local_path": file_path,
            "status": status,
            "error": error,
            "metrics": metrics or {}
        }
        
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    
    def _save_metadata(self):
        metadata_entry = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.start_time.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "root_dir": str(ROOT_DIR),
            "total_processed": len(self.execution_stats["processed"]),
            "total_errors": len(self.execution_stats["errors"]),
            "profiles": self.execution_stats["processed"],
            "errors": self.execution_stats["errors"]
        }
        
        with open(self.metadata_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(metadata_entry, ensure_ascii=False) + "\n")
        
        self._log_info(f"   Metadata: {self.metadata_path}")
    
    def _show_summary(self):
        print(f"\n{'='*70}")
        print(f"RESUMEN DE ANÁLISIS DE CALIDAD")
        print(f"{'='*70}")
        print(f"Pipeline ID: {self.pipeline_id}")
        print(f"Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'-'*70}")
        
        if self.execution_stats["processed"]:
            print(f"Procesados: {len(self.execution_stats['processed'])}")
            for item in self.execution_stats["processed"]:
                status = "✅" if item["passed"] else "⚠️"
                print(f"   {status} - {item['taxi_type']} {item['year']}: Score {item['quality_score']:.2%} ({item['records']:,} registros)")
        
        if self.execution_stats["errors"]:
            print(f"Errores: {len(self.execution_stats['errors'])}")
            for item in self.execution_stats["errors"]:
                print(f"   - {item['taxi_type']} {item['year']}: {item['error']}")
        
        print(f"\nUbicaciones:")
        print(f"   Reports: {self.reports_dir}")
        print(f"   Metadata: {self.metadata_dir}")
        print(f"   Logs: {self.log_path}")
        print(f"{'='*70}")


def main():
    parser = argparse.ArgumentParser(
        description="Análisis de calidad - Produccion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EJEMPLOS:
  # Procesar TODOS los tipos activos
  python quality_analysis.py --taxi all --years 2023 2024 2025
  
  # Procesar solo Yellow 2023
  python quality_analysis.py --taxi yellow --years 2023
  
  # Forzar reprocesamiento
  python quality_analysis.py --taxi yellow --years 2023 --force
        """
    )
    
    parser.add_argument("--taxi", nargs="+",
                       choices=["fhv", "fhvhv", "green", "yellow", "all"],
                       default=["all"],
                       help="Tipos de taxi a procesar (default: all)")
    
    parser.add_argument("--years", nargs="+", type=int,
                       default=[2023, 2024, 2025],
                       help="Años a procesar (default: 2023 2024 2025)")
    
    parser.add_argument("--force", action="store_true",
                       help="Forzar reprocesamiento")
    
    args = parser.parse_args()
    
    if "all" in args.taxi:
        taxi_types = TAXI_TYPES_ACTIVE.copy()
        print("[INFO] NOTA: 'all' excluye FHVHV (demasiado grande)")
    else:
        taxi_types = args.taxi
    
    spark = SparkSession.builder \
        .appName("QualityAnalyzer") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    print(f"\nIniciando análisis de calidad...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Tipos a procesar: {taxi_types}")
    
    analyzer = QualityAnalyzer(spark)
    analyzer.process(taxi_types, args.years, args.force)
    
    print(f"\nANÁLISIS DE CALIDAD COMPLETADO")
    print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()