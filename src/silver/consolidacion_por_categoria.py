"""
============================================================
PIPELINE SILVER
Consolidación por categoría y año

Arquitectura Silver

silver
└───consolidated
    ├───lookup
    │   └───taxi_zone_lookup
    ├───trip_data
    │   ├───tipo_dataset=fhv
    │   │   ├───anio=2023
    │   │   ├───anio=2024
    │   │   └───anio=2025
    │   ├───tipo_dataset=fhvhv
    │   │   ├───anio=2023
    │   │   ├───anio=2024
    │   │   └───anio=2025
    │   ├───tipo_dataset=green
    │   │   ├───anio=2023
    │   │   ├───anio=2024
    │   │   └───anio=2025
    │   └───tipo_dataset=yellow
    │       ├───anio=2023
    │       ├───anio=2024
    │       └───anio=2025
    └───_metadata

Cada año contiene un único parquet consolidado.
============================================================
"""

from pathlib import Path
import shutil
import json
import os
import time
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from pyspark.sql.types import (
    IntegerType,
    LongType,
    DoubleType,
    StringType,
    TimestampType,
)

# ==========================================================
# RUTAS
# ==========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA = PROJECT_ROOT / "data"

BRONZE = DATA / "bronze"
SILVER = DATA / "silver"

SILVER_CONSOLIDATED = (
    SILVER
    / "consolidated"
)

LOGS = DATA / "logs"

SILVER_METADATA = (
    SILVER_CONSOLIDATED
    / "_metadata"
)

BRONZE_TRIPS = (
    BRONZE
    / "files"
    / "trip_data"
)

BRONZE_LOOKUP = (
    BRONZE
    / "files"
    / "lookup"
)

SILVER_TRIPS = (
    SILVER_CONSOLIDATED
    / "trip_data"
)

SILVER_LOOKUP = (
    SILVER_CONSOLIDATED
    / "lookup"
)

# ==========================================================
# CONFIGURACIÓN
# ==========================================================

CATEGORIAS = [

    "yellow",
    "green",
    "fhv",
    "fhvhv"

]

CONFIG = {

    "compression": "snappy",

    "coalesce": 1,

    "shuffle_partitions": 8,

    "timezone": "UTC",

    "adaptive": True,

    "ignore_corrupt_files": True,

    "ignore_missing_files": True,

}

# ==========================================================
# SPARK
# ==========================================================

def create_spark() -> SparkSession:

    spark = (

        SparkSession.builder

        .master("local[*]")

        .appName("Silver_Consolidacion")

        .config(
            "spark.sql.session.timeZone",
            CONFIG["timezone"]
        )

        .config(
            "spark.sql.shuffle.partitions",
            CONFIG["shuffle_partitions"]
        )

        .config(
            "spark.sql.parquet.compression.codec",
            CONFIG["compression"]
        )

        .config(
            "spark.sql.files.ignoreCorruptFiles",
            str(CONFIG["ignore_corrupt_files"]).lower()
        )

        .config(
            "spark.sql.files.ignoreMissingFiles",
            str(CONFIG["ignore_missing_files"]).lower()
        )

        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )

        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )

        .getOrCreate()

    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

# ==========================================================
# UTILIDADES
# ==========================================================

def mostrar_titulo(texto: str):

    print()
    print("=" * 80)
    print(texto)
    print("=" * 80)


def crear_directorio(path: Path):

    path.mkdir(
        parents=True,
        exist_ok=True
    )


def eliminar_directorio(path: Path):

    if path.exists():

        shutil.rmtree(path)


def listar_anios(categoria: str):

    ruta = (
        BRONZE_TRIPS
        / f"tipo_dataset={categoria}"
    )

    if not ruta.exists():

        return []

    return sorted(

        carpeta.name.replace("anio=", "")

        for carpeta in ruta.iterdir()

        if carpeta.is_dir()

    )

# ==========================================================
# CONTROL INCREMENTAL
# ==========================================================

def silver_existe(
    categoria: str,
    anio: str
) -> bool:

    salida = (

        SILVER_TRIPS

        / f"tipo_dataset={categoria}"

        / f"anio={anio}"

    )

    return salida.exists()

# ==========================================================
# NORMALIZACIÓN DE ESQUEMA
# ==========================================================

def normalizar_dataframe(
    df: DataFrame
) -> DataFrame:

    """
    Convierte los tipos incompatibles para
    evitar errores entre meses.
    """

    for campo in df.schema.fields:

        nombre = campo.name

        tipo = campo.dataType

        if isinstance(tipo, IntegerType):

            df = df.withColumn(
                nombre,
                col(nombre).cast(LongType())
            )

        elif isinstance(tipo, DoubleType):

            df = df.withColumn(
                nombre,
                col(nombre).cast(DoubleType())
            )

        elif isinstance(tipo, StringType):

            df = df.withColumn(
                nombre,
                col(nombre).cast(StringType())
            )

        elif isinstance(tipo, TimestampType):

            df = df.withColumn(
                nombre,
                col(nombre).cast(TimestampType())
            )

    return df

# ==========================================================
# METADATA
# ==========================================================

def guardar_metadata(
    categoria: str,
    anio: str,
    registros_entrada: int,
    registros_salida: int,
    columnas: int,
    ruta_salida: Path
):

    crear_directorio(SILVER_METADATA)


    metadata = {

        "pipeline_id":
            f"silver_consolidacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}",

        "pipeline_name":
            "silver_consolidacion_por_categoria",

        "pipeline_version":
            "1.0.0",

        "capa":
            "silver",

        "categoria":
            categoria,

        "anio":
            anio,

        "fecha_generacion":
            datetime.now().isoformat(),

        "registros_entrada":
            registros_entrada,

        "registros_salida":
            registros_salida,

        "columnas":
            columnas,

        "compression":
            CONFIG["compression"],

        "silver_path":
            str(ruta_salida),

        "status":
            "SUCCESS"

    }


    archivo = (
        SILVER_METADATA /
        f"silver_{categoria}_{anio}.json"
    )


    with open(
        archivo,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            metadata,
            f,
            indent=4,
            ensure_ascii=False
        )

# ==========================================================
# RENOMBRAR PARQUET
# ==========================================================

def renombrar_parquet(
    carpeta: Path,
    nombre_final: str
):

    archivos = list(
        carpeta.glob("part-*.parquet")
    )

    if not archivos:
        return

    destino = carpeta / nombre_final

    if destino.exists():
        destino.unlink()

    archivos[0].rename(destino)

    for archivo in carpeta.glob("*.crc"):
        archivo.unlink()

    success = carpeta / "_SUCCESS"

    if success.exists():
        success.unlink()

# ==========================================================
# LECTURA DE UN AÑO
# ==========================================================

def leer_anio(
    spark: SparkSession,
    categoria: str,
    anio: str
) -> DataFrame:

    ruta_anio = (
        BRONZE_TRIPS
        / f"tipo_dataset={categoria}"
        / f"anio={anio}"
    )

    mostrar_titulo(f"{categoria.upper()} - {anio}")

    carpetas_mes = sorted(
        carpeta
        for carpeta in ruta_anio.iterdir()
        if carpeta.is_dir()
    )

    if not carpetas_mes:
        raise FileNotFoundError(
            f"No existen datos para {categoria} {anio}"
        )

    dataframes = []

    for carpeta_mes in carpetas_mes:

        print(f"Leyendo {carpeta_mes.name}")

        df = (
            spark.read
            .option("mergeSchema", "false")
            .parquet(str(carpeta_mes))
        )

        df = normalizar_dataframe(df)

        dataframes.append(df)

    df_final = dataframes[0]

    for df in dataframes[1:]:

        df_final = df_final.unionByName(
            df,
            allowMissingColumns=True
        )

    return df_final


# ==========================================================
# ESCRITURA SILVER
# ==========================================================

def guardar_parquet(
    df: DataFrame,
    destino: Path,
    nombre_archivo: str
):

    eliminar_directorio(destino)

    crear_directorio(destino.parent)

    (
        df
        .coalesce(CONFIG["coalesce"])
        .write
        .mode("overwrite")
        .option(
            "compression",
            CONFIG["compression"]
        )
        .parquet(str(destino))
    )
    renombrar_parquet(destino, nombre_archivo)


def escribir_anio(
    df: DataFrame,
    categoria: str,
    anio: str,
    registros_entrada:int
):

    salida = (
        SILVER_TRIPS
        / f"tipo_dataset={categoria}"
        / f"anio={anio}"
    )

    registros_salida = df.count()

    guardar_parquet(df,salida,f"{categoria}_tripdata_{anio}.parquet")

    guardar_metadata(
            categoria,
            anio,
            registros_entrada,
            registros_salida,
            len(df.columns),
            salida
    )

    print(f"✔ Silver generado -> {salida}")

# ==========================================================
# PROCESAMIENTO POR CATEGORÍA
# ==========================================================

def procesar_categoria(
    spark: SparkSession,
    categoria: str
) -> None:

    mostrar_titulo(f"PROCESANDO {categoria.upper()}")

    ruta_categoria = BRONZE_TRIPS / f"tipo_dataset={categoria}"

    if not ruta_categoria.exists():
        print(f"No existe la carpeta {ruta_categoria}")
        return

    anios = listar_anios(categoria)

    print("Años encontrados:", anios)

    for anio in anios:
        if silver_existe(categoria, anio):
            print(f"✔ Silver {categoria} {anio} ya existe.")
            continue

        print(f"\nProcesando año {anio}...")

        inicio = time.time()

        df = leer_anio(
            spark,
            categoria,
            anio
        )

        registros_entrada = df.count()

        escribir_anio(
            df,
            categoria,
            anio,
            registros_entrada
        )

        print(
            f"Tiempo: {time.time() - inicio:.2f} segundos"
        )


# ==========================================================
# LOOKUP
# ==========================================================

def procesar_lookup(spark: SparkSession) -> None:

    origen = BRONZE_LOOKUP / "taxi_zone_lookup.csv"

    destino = (
        SILVER_LOOKUP
        / "taxi_zone_lookup"
    )

    eliminar_directorio(destino)

    crear_directorio(destino.parent)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(origen))
    )

    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", CONFIG["compression"])
        .parquet(str(destino))
    )

    renombrar_parquet(destino,"taxi_zone_lookup.parquet")

    print("\nLookup convertido a Parquet")
    print(destino)

# ==========================================================
# LOGS
# ==========================================================

def guardar_log_silver(log):
    crear_directorio(LOGS)
    archivo = (
        LOGS /
        "silver_consolidacion_categoria_manifest.jsonl"
    )

    with open(
        archivo,
        "a",
        encoding="utf-8"
    ) as f:

        f.write(
            json.dumps(
                log,
                ensure_ascii=False,
                default=str
            )
            + "\n"
        )

# ==========================================================
# MAIN
# ==========================================================

def main() -> None:

    mostrar_titulo("PIPELINE SILVER")

    crear_directorio(SILVER_TRIPS)

    spark = create_spark()

    inicio_total = time.time()

    inicio = datetime.now()

    pipeline_id = (
        f"silver_consolidacion_"
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )

    try:

        for categoria in CATEGORIAS:

            procesar_categoria(
                spark,
                categoria
            )

        procesar_lookup(spark)

        mostrar_titulo("PIPELINE FINALIZADO")

        print(
            f"Tiempo total: {time.time() - inicio_total:.2f} segundos"
        )

        guardar_log_silver({
            "pipeline_id": pipeline_id,

            "pipeline_name":
            "silver_consolidacion_por_categoria",

            "capa":
            "silver",

            "inicio":
            inicio,

            "fin":
            datetime.now(),

            "duracion_segundos":
            round(
                time.time()-inicio_total,
                2
            ),

            "status":
            "SUCCESS",

            "error":
            None
        })
    finally:

        spark.stop()


# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":
    main()