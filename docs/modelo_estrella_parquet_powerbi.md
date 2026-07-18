# Modelo estrella Gold en Parquet para Power BI

## Objetivo

El script `src/gold/build_modelo_estrella.py` transforma los conjuntos normalizados de `data/silver/trip_data_normalized` en dimensiones y una tabla de hechos agregada almacenadas en Parquet. El notebook `notebooks/gold/modelo_estrella_parquet_powerbi.ipynb` se conserva como referencia exploratoria.

El flujo implementado es:

```text
Nuevo Parquet normalizado
        -> staging incremental por fuente
        -> dimensiones y hechos Gold
        -> Power BI Gateway
        -> actualizacion del modelo semantico
        -> dashboards actualizados
```

MongoDB ya no forma parte obligatoria del flujo. Las relaciones se definen en Power BI y se conservan durante las actualizaciones mientras los nombres y tipos de columnas no cambien.

## Salidas

La primera ejecucion crea:

```text
data/gold/modelo_estrella/
|-- dimensiones/
|   |-- dim_tiempo.parquet/
|   |-- dim_hora.parquet/
|   |-- dim_tipo_taxi.parquet/
|   |-- dim_zona_pickup.parquet/
|   |-- dim_zona_dropoff.parquet/
|   |-- dim_pago.parquet/
|   |-- dim_ratecode.parquet/
|   |-- dim_trip_type.parquet/
|   |-- dim_shared_ride.parquet/
|   `-- dim_proveedor.parquet/
|-- hechos/
|   `-- fact_viajes_agregados.parquet/
|-- _control/
|   `-- process_manifest.json
`-- _staging/
    `-- sources/
```

Las rutas terminadas en `.parquet` son carpetas Spark con uno o varios archivos Parquet internos. `_control` y `_staging` son tecnicos y no se cargan en Power BI.

## Granularidad de la tabla de hechos

Cada fila de `fact_viajes_agregados` representa una combinacion de:

```text
time_id
hora_id
tipo_dataset
pickup_location_id
dropoff_location_id
payment_type
ratecode_id
trip_type
shared_ride_flag
provider_key
```

La tabla contiene cantidades, totales y promedios. No contiene un `_id` textual por fila, porque no es necesario para las relaciones y aumenta el tamano del modelo de Power BI.

## Dimension temporal

`dim_tiempo` tiene una fila por mes y anio presentes en los datos:

```text
time_id
fecha_inicio_mes
anio
mes
nombre_mes
trimestre
etiqueta_mes
orden_mes
```

Ejemplo:

```text
2024-07 | 2024-07-01 | 2024 | 7 | Julio | 3 | Julio 2024 | 202407
```

En Power BI se debe ordenar `nombre_mes` por `mes` y `etiqueta_mes` por `orden_mes`.

## Control incremental

El manifiesto registra cada entrada Silver mediante su ruta y una huella basada en nombres de partes, tamanos y fechas de modificacion.

En una ejecucion normal:

1. Se comparan las entradas actuales con el manifiesto.
2. Solo se leen desde Silver las fuentes nuevas o modificadas.
3. Cada fuente genera una contribucion agregada independiente en `_staging/sources`.
4. Si una fuente cambia, su contribucion anterior se reemplaza.
5. Las contribuciones se consolidan para publicar las mismas rutas Gold.
6. El manifiesto se actualiza unicamente despues de publicar Gold correctamente.

Este mecanismo evita volver a leer todos los viajes crudos y evita duplicar una fuente reprocesada. La consolidacion final se vuelve a escribir para que Power BI siempre encuentre una tabla coherente.

## Primera ejecucion

1. Ejecutar `python src/gold/build_modelo_estrella.py --taxi all --years 2026` desde la raíz del proyecto.
2. Esperar el mensaje `Gold publicado`.
3. Revisar `data/logs/gold_parquet_audit.jsonl` si ocurre un error.

Para forzar el reproceso de las entradas seleccionadas y republicar el modelo:

```powershell
python src/gold/build_modelo_estrella.py --taxi all --years 2026 --force-reprocess --force-rebuild
```

La primera ejecucion procesa todas las fuentes porque el manifiesto aun no existe. Las siguientes procesan solamente entradas nuevas o modificadas.

## Carga en Power BI Desktop

Para cada tabla:

1. Seleccionar **Obtener datos > Carpeta**.
2. Elegir exactamente la carpeta de la tabla, por ejemplo `data/gold/modelo_estrella/dimensiones/dim_tiempo.parquet`.
3. Seleccionar **Transformar datos**.
4. Filtrar la columna `Extension` para conservar solo `.parquet`.
5. Seleccionar **Combinar archivos**.
6. Asignar a la consulta el nombre de la tabla.

No se debe seleccionar la raiz `modelo_estrella`, porque tambien contiene staging tecnico.

## Relaciones en Power BI

Todas las relaciones son `1:*`, con filtro simple desde la dimension hacia los hechos:

| Dimension | Clave | Clave en `fact_viajes_agregados` |
|---|---|---|
| `dim_tiempo` | `time_id` | `time_id` |
| `dim_hora` | `hora_id` | `hora_id` |
| `dim_tipo_taxi` | `tipo_dataset` | `tipo_dataset` |
| `dim_zona_pickup` | `pickup_location_id` | `pickup_location_id` |
| `dim_zona_dropoff` | `dropoff_location_id` | `dropoff_location_id` |
| `dim_pago` | `payment_type` | `payment_type` |
| `dim_ratecode` | `ratecode_id` | `ratecode_id` |
| `dim_trip_type` | `trip_type` | `trip_type` |
| `dim_shared_ride` | `shared_ride_flag` | `shared_ride_flag` |
| `dim_proveedor` | `provider_key` | `provider_key` |

## Automatizacion local de Gold

La alternativa heredada `scripts/run_gold_notebook.ps1` ejecuta el notebook con Jupyter. Para el pipeline y las ejecuciones normales utiliza el script Python.

Prueba manual heredada:

```powershell
powershell.exe -ExecutionPolicy Bypass -File ".\scripts\run_gold_notebook.ps1"
```

Para programarlo en Windows:

1. Abrir **Programador de tareas**.
2. Crear una tarea y seleccionar **Ejecutar tanto si el usuario inicio sesion como si no**.
3. En **Desencadenadores**, configurar el horario de procesamiento Gold.
4. En **Acciones**, usar `powershell.exe` como programa.
5. Usar como argumentos `-ExecutionPolicy Bypass -File "RUTA_COMPLETA\scripts\run_gold_notebook.ps1"`.
6. Evitar ejecuciones simultaneas en la configuracion de la tarea.

La computadora debe permanecer encendida durante el procesamiento.

## Power BI Service y Gateway

1. Instalar y registrar On-premises Data Gateway en la misma computadora.
2. Mantener disponible la ruta local usada en Power BI Desktop.
3. Publicar el archivo PBIX en Power BI Service.
4. Abrir la configuracion del modelo semantico.
5. Asociar las fuentes de carpeta con el Gateway.
6. Configurar la actualizacion de Power BI despues del horario de Gold.

Ejemplo:

```text
01:00  Ejecutar Gold
02:00  Actualizar Power BI
```

El Gateway y la computadora deben estar activos. La presencia de un nuevo Parquet no actualiza Power BI por evento; el dashboard cambia cuando termina la actualizacion manual o programada del modelo semantico.

## Nuevos archivos Silver

Un nuevo conjunto debe colocarse dentro de:

```text
data/silver/trip_data_normalized
```

El nombre puede seguir el patron existente, por ejemplo:

```text
yellow_2026.parquet
green_2026.parquet
fhv_2026.parquet
```

El archivo debe conservar el esquema canonico creado por Silver. Despues se ejecuta Gold y, finalmente, la actualizacion de Power BI.
