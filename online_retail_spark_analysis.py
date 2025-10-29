# =====================================================
# Análisis de Ventas Online con Apache Spark
# =====================================================
# Estudiante: Brayan Perez
# Proyecto: Procesamiento Batch en Spark - Análisis Big Data
# Dataset: Online Retail Dataset (Kaggle)
# =====================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, countDistinct, round, desc, avg, to_date

# 1️ Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("OnlineRetailSparkAnalysis") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# 2️ Cargar el conjunto de datos
# Asegúrate de colocar el archivo "Online_Data.csv" en el mismo directorio o usar una ruta absoluta
df = spark.read.option("header", "true").option("inferSchema", "true").csv("Online_Data.csv")

# 3️ Inspeccionar los datos
print(" Estructura inicial del dataset:")
df.printSchema()
print(f"Total de registros: {df.count()}")

# 4️ Limpieza de datos
# Eliminar filas con valores nulos en columnas clave
df_clean = df.dropna(subset=["InvoiceNo", "Description", "Quantity", "UnitPrice", "CustomerID"])

# Eliminar registros con cantidades o precios negativos
df_clean = df_clean.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))

# Convertir fecha a tipo date y crear columna de valor total
df_clean = df_clean.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "MM/dd/yyyy")) \
                   .withColumn("TotalPrice", round(col("Quantity") * col("UnitPrice"), 2))

print(" Datos limpios y preparados:")
print(f"Total de registros limpios: {df_clean.count()}")

# 5️ Análisis 1: Top 10 productos más vendidos
top_products = df_clean.groupBy("Description") \
                       .agg(spark_sum("Quantity").alias("TotalQuantity")) \
                       .orderBy(desc("TotalQuantity"))

print("\n Top 10 productos más vendidos:")
top_products.show(10, truncate=False)

# 6️ Análisis 2: Ingresos totales por país
sales_by_country = df_clean.groupBy("Country") \
                           .agg(round(spark_sum("TotalPrice"), 2).alias("TotalRevenue")) \
                           .orderBy(desc("TotalRevenue"))

print("\n Ingresos totales por país:")
sales_by_country.show(10, truncate=False)

# 7️ Análisis 3: Clientes más rentables
top_customers = df_clean.groupBy("CustomerID") \
                        .agg(round(spark_sum("TotalPrice"), 2).alias("TotalSpent"),
                             countDistinct("InvoiceNo").alias("NumPurchases")) \
                        .orderBy(desc("TotalSpent"))

print("\n Top 10 clientes más rentables:")
top_customers.show(10, truncate=False)

# 8️ Análisis 4: Promedio de ticket por transacción
avg_ticket = df_clean.groupBy("InvoiceNo") \
                     .agg(round(spark_sum("TotalPrice"), 2).alias("InvoiceTotal"))

ticket_mean = avg_ticket.agg(round(avg("InvoiceTotal"), 2).alias("AvgTicketValue")).collect()[0][0]
print(f"\n Valor promedio de factura: {ticket_mean}")

# 9️ Guardar los resultados
top_products.write.mode("overwrite").option("header", "true").csv("output/top_products")
sales_by_country.write.mode("overwrite").option("header", "true").csv("output/sales_by_country")
top_customers.write.mode("overwrite").option("header", "true").csv("output/top_customers")

print("\n Resultados exportados a la carpeta 'output/' en formato CSV.")

# 10 Finalizar sesión de Spark
spark.stop()
print("\n Proceso completado exitosamente.")
