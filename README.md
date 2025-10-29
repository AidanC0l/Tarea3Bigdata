#  Análisis de Ventas Online con Apache Spark

##  Descripción general

Este proyecto implementa una **solución Big Data con Apache Spark** enfocada en el **procesamiento batch de datos de ventas online**.  
El objetivo principal es analizar un conjunto de datos de transacciones minoristas para identificar patrones de compra, productos más vendidos, ingresos por país y clientes más rentables.

El análisis se realiza sobre el **Online Retail Dataset (Kaggle)**, el cual contiene cientos de miles de registros de ventas de una tienda online europea entre 2009 y 2011.  
Se emplea **PySpark** como motor de procesamiento distribuido para garantizar escalabilidad, eficiencia y tolerancia a fallos.

---

##  Arquitectura de la solución

La solución se desarrolla bajo una arquitectura **batch de Spark** compuesta por las siguientes etapas:

```text
📂 Online_Data.csv (Fuente de datos)
       ↓
🚀 SparkSession (Ingesta y carga de datos)
       ↓
🧹 Limpieza y transformación
       - Eliminación de nulos
       - Filtrado de valores negativos
       - Cálculo de columnas derivadas (TotalPrice)
       ↓
📈 Procesamiento distribuido
       - Agrupaciones y agregaciones
       - Cálculo de KPIs: top productos, ingresos, clientes
       ↓
💾 Almacenamiento de resultados
       - Resultados exportados en formato CSV (carpeta /output)
