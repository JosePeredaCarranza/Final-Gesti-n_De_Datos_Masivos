# Arquitectura del Proyecto TLC

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
