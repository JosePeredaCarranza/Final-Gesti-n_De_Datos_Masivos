# Modelo estrella Gold para MongoDB y Power BI

Este documento describe el modelo Gold creado para consumir los datos TLC desde MongoDB en Power BI. El modelo parte de los datos normalizados en `data/silver/trip_data_normalized/` y construye una capa analitica agregada, evitando cargar los viajes crudos completos en la herramienta de visualizacion.

## Objetivo del modelo

El objetivo es transformar los viajes normalizados de la capa Silver en un modelo estrella listo para analisis descriptivo y diagnostico.

El flujo queda organizado asi:

```text
Bronze -> Silver cleaned -> Silver normalized -> Gold modelo estrella -> MongoDB -> Power BI
```

MongoDB funciona como capa de serving o consumo. Power BI se conecta a las colecciones Gold ya agregadas.

## Tabla de hechos

La tabla principal es:

```text
fact_viajes_agregados
```

Esta tabla no guarda cada viaje individual. Guarda viajes agregados por una granularidad analitica definida.

### Granularidad

Cada fila representa una combinacion de:

```text
anio
pickup_month
pickup_hour
tipo_dataset
pickup_location_id
dropoff_location_id
payment_type
ratecode_id
trip_type
shared_ride_flag
provider_key
```

En terminos de negocio, cada fila responde:

> Para un mes, hora, tipo de taxi, ruta, forma de pago, tarifa, tipo de viaje y proveedor/base, cuantos viajes hubo y cuales fueron sus metricas agregadas.

### Clave tecnica

La columna `_id` se genera con un hash SHA-256 sobre las columnas de granularidad. Esto permite cargar la tabla de forma idempotente en MongoDB.

### Metricas

La tabla de hechos contiene metricas agregadas como:

| Columna | Descripcion |
|---|---|
| `cantidad_viajes` | Numero total de viajes en la combinacion agregada. |
| `total_pasajeros` | Suma de pasajeros registrados. |
| `total_distancia_millas` | Suma de distancia recorrida en millas. |
| `total_duracion_minutos` | Suma de duracion de viajes en minutos. |
| `total_monto_base` | Suma de tarifa base. |
| `total_monto_total` | Suma del monto total cobrado. |
| `total_propina` | Suma de propinas. |
| `total_peajes` | Suma de peajes. |
| `total_extra` | Suma de cargos extra. |
| `total_mta_tax` | Suma del impuesto MTA. |
| `total_improvement_surcharge` | Suma del improvement surcharge. |
| `total_congestion_surcharge` | Suma del congestion surcharge. |
| `total_airport_fee` | Suma de airport fee. |
| `total_cbd_congestion_fee` | Suma de CBD congestion fee. |
| `total_ehail_fee` | Suma de e-hail fee. |
| `promedio_distancia_millas` | Distancia promedio por viaje. |
| `promedio_duracion_minutos` | Duracion promedio por viaje. |
| `promedio_monto_total` | Monto promedio por viaje. |
| `promedio_propina` | Propina promedio por viaje. |

Para FHV, algunos campos monetarios o de pago no existen en el diccionario oficial. En esos casos se manejan como valores nulos en Silver y como cero en metricas sumadas de Gold, sin inventar datos inexistentes.

## Dimensiones

Las dimensiones convierten codigos tecnicos en categorias interpretables para Power BI.

## `dim_anio`

Dimension de anios. Cada anio aparece una sola vez.

| Columna | Descripcion |
|---|---|
| `_id` | Identificador del anio. |
| `anio` | Anio del viaje. |
| `etiqueta_anio` | Anio como texto para visuales. |

Relacion:

```text
dim_anio.anio -> fact_viajes_agregados.anio
```

## `dim_mes`

Dimension de meses. Cada mes aparece una sola vez, por lo que `Enero` no se repite para cada anio.

| Columna | Descripcion |
|---|---|
| `_id` | Numero del mes. |
| `mes` | Numero del mes. |
| `pickup_month` | Clave usada para relacionar con la tabla de hechos. |
| `nombre_mes` | Nombre del mes. |
| `mes_corto` | Nombre abreviado del mes. |
| `orden_mes` | Orden numerico para Power BI. |
| `trimestre` | Trimestre del anio. |

Relacion:

```text
dim_mes.pickup_month -> fact_viajes_agregados.pickup_month
```

En Power BI, si se requiere analizar una serie temporal cronologica, se deben usar juntos `dim_anio` y `dim_mes`. Si se usa solo `dim_mes`, Power BI agrupara todos los eneros de todos los anios.

## `dim_hora`

Dimension para analizar demanda por hora del dia.

| Columna | Descripcion |
|---|---|
| `_id` | Hora numerica. |
| `pickup_hour` | Hora de inicio del viaje. |
| `hora` | Etiqueta de hora, por ejemplo `08:00`. |
| `franja` | Franja horaria: Madrugada, Manana, Tarde o Noche. |

Relacion:

```text
dim_hora.pickup_hour -> fact_viajes_agregados.pickup_hour
```

## `dim_tipo_taxi`

Dimension de tipo de servicio.

| Columna | Descripcion |
|---|---|
| `_id` | Codigo del tipo de dataset. |
| `tipo_dataset` | `yellow`, `green` o `fhv`. |
| `tipo_taxi` | Nombre descriptivo del servicio. |

Valores principales:

| tipo_dataset | tipo_taxi |
|---|---|
| `yellow` | Yellow Taxi |
| `green` | Green Taxi / SHL |
| `fhv` | For-Hire Vehicle |

Relacion:

```text
dim_tipo_taxi.tipo_dataset -> fact_viajes_agregados.tipo_dataset
```

## `dim_zona_pickup`

Dimension de zona de origen del viaje, basada en `taxi_zone_lookup`.

| Columna | Descripcion |
|---|---|
| `_id` | Identificador de zona TLC. |
| `pickup_location_id` | Zona donde inicio el viaje. |
| `pickup_borough` | Borough de origen. |
| `pickup_zone` | Nombre de la zona de origen. |
| `pickup_service_zone` | Zona de servicio TLC. |

Relacion:

```text
dim_zona_pickup.pickup_location_id -> fact_viajes_agregados.pickup_location_id
```

## `dim_zona_dropoff`

Dimension de zona de destino del viaje, basada en `taxi_zone_lookup`.

| Columna | Descripcion |
|---|---|
| `_id` | Identificador de zona TLC. |
| `dropoff_location_id` | Zona donde termino el viaje. |
| `dropoff_borough` | Borough de destino. |
| `dropoff_zone` | Nombre de la zona de destino. |
| `dropoff_service_zone` | Zona de servicio TLC. |

Relacion:

```text
dim_zona_dropoff.dropoff_location_id -> fact_viajes_agregados.dropoff_location_id
```

## `dim_pago`

Dimension de forma de pago, basada en los diccionarios oficiales de Yellow y Green Taxi.

| payment_type | Descripcion |
|---|---|
| `-1` | No informado / No aplica |
| `0` | Flex Fare trip |
| `1` | Credit card |
| `2` | Cash |
| `3` | No charge |
| `4` | Dispute |
| `5` | Unknown |
| `6` | Voided trip |

Relacion:

```text
dim_pago.payment_type -> fact_viajes_agregados.payment_type
```

## `dim_ratecode`

Dimension de tarifa aplicada, basada en los diccionarios oficiales de Yellow y Green Taxi.

| ratecode_id | Descripcion |
|---|---|
| `-1` | No informado / No aplica |
| `1` | Standard rate |
| `2` | JFK |
| `3` | Newark |
| `4` | Nassau or Westchester |
| `5` | Negotiated fare |
| `6` | Group ride |
| `99` | Null / unknown |

Relacion:

```text
dim_ratecode.ratecode_id -> fact_viajes_agregados.ratecode_id
```

## `dim_trip_type`

Dimension especifica para Green Taxi. Indica si el viaje fue street-hail o dispatch.

| trip_type | Descripcion |
|---|---|
| `-1` | No informado / No aplica |
| `1` | Street-hail |
| `2` | Dispatch |

Relacion:

```text
dim_trip_type.trip_type -> fact_viajes_agregados.trip_type
```

## `dim_shared_ride`

Dimension especifica para FHV. Se basa en `SR_Flag`, que indica si el viaje fue parte de una cadena de viaje compartido.

| shared_ride_flag | Descripcion |
|---|---|
| `-1` | No aplica |
| `0` | No compartido / no informado |
| `1` | Compartido |

Relacion:

```text
dim_shared_ride.shared_ride_flag -> fact_viajes_agregados.shared_ride_flag
```

## `dim_proveedor`

Dimension para proveedores o bases. En Yellow y Green representa vendor; en FHV representa base.

| Columna | Descripcion |
|---|---|
| `_id` | Clave tecnica del proveedor. |
| `provider_key` | Clave compuesta `tipo_dataset|provider_id`. |
| `provider_id` | Codigo de vendor o base FHV. |
| `tipo_dataset` | Tipo de servicio asociado. |
| `tipo_proveedor` | `Taxi vendor` o `FHV base`. |
| `proveedor` | Nombre descriptivo cuando existe en el diccionario. |

Catalogo base para vendors:

| vendor_id | Proveedor |
|---|---|
| `1` | Creative Mobile Technologies, LLC |
| `2` | Curb Mobility, LLC |
| `6` | Myle Technologies Inc |
| `7` | Helix |

Relacion:

```text
dim_proveedor.provider_key -> fact_viajes_agregados.provider_key
```

## Relaciones del modelo en Power BI

Configurar relaciones 1 a muchos desde cada dimension hacia la tabla de hechos:

| Dimension | Columna | Tabla de hechos | Columna |
|---|---|---|---|
| `dim_anio` | `anio` | `fact_viajes_agregados` | `anio` |
| `dim_mes` | `pickup_month` | `fact_viajes_agregados` | `pickup_month` |
| `dim_hora` | `pickup_hour` | `fact_viajes_agregados` | `pickup_hour` |
| `dim_tipo_taxi` | `tipo_dataset` | `fact_viajes_agregados` | `tipo_dataset` |
| `dim_zona_pickup` | `pickup_location_id` | `fact_viajes_agregados` | `pickup_location_id` |
| `dim_zona_dropoff` | `dropoff_location_id` | `fact_viajes_agregados` | `dropoff_location_id` |
| `dim_pago` | `payment_type` | `fact_viajes_agregados` | `payment_type` |
| `dim_ratecode` | `ratecode_id` | `fact_viajes_agregados` | `ratecode_id` |
| `dim_trip_type` | `trip_type` | `fact_viajes_agregados` | `trip_type` |
| `dim_shared_ride` | `shared_ride_flag` | `fact_viajes_agregados` | `shared_ride_flag` |
| `dim_proveedor` | `provider_key` | `fact_viajes_agregados` | `provider_key` |

## Medidas sugeridas para Power BI

```DAX
Viajes = SUM(fact_viajes_agregados[cantidad_viajes])

Ingresos = SUM(fact_viajes_agregados[total_monto_total])

Distancia = SUM(fact_viajes_agregados[total_distancia_millas])

Duracion = SUM(fact_viajes_agregados[total_duracion_minutos])

Ticket Promedio = DIVIDE([Ingresos], [Viajes])

Distancia Promedio = DIVIDE([Distancia], [Viajes])

Duracion Promedio = DIVIDE([Duracion], [Viajes])

Propina Promedio = DIVIDE(SUM(fact_viajes_agregados[total_propina]), [Viajes])
```

## Justificacion dentro de Medallion + Kappa

El modelo se ubica en la capa Gold de la arquitectura Medallion:

```text
Silver normalized -> Gold modelo estrella -> MongoDB -> Power BI
```

La logica respeta un enfoque Kappa batch-reprocesable porque no existen dos pipelines paralelos para historico y datos recientes. La misma ruta transforma cualquier periodo disponible desde Silver hasta Gold, permitiendo reejecutar el proceso cuando se incorporen nuevos meses o anios.
