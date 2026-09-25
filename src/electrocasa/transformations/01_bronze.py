# Databricks notebook source

# COMMAND ----------
import os
from pyspark.sql.functions import current_timestamp, input_file_name, lit

catalog_name = "catalogo_electrocasa"
volume_base = f"/Volumes/{catalog_name}/bronze/datos_proyecto"

# Asegurar que el esquema bronze exista
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze;")

# Función auxiliar para verificar si una carpeta tiene archivos antes de leer
def carpeta_tiene_archivos(path):
    try:
        return len(os.listdir(path)) > 0
    except Exception:
        return False

# COMMAND ----------
# 1. VENTAS POR SUCURSAL (CSV)
path_ventas = f"{volume_base}/landing/ventas/"

if carpeta_tiene_archivos(path_ventas):
    print("Ingestando Ventas...")
    df_ventas = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path_ventas)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    df_ventas.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog_name}.bronze.bronze_ventas")
    print("Ventas ingestadas correctamente.")
else:
    print(f"ADVERTENCIA: La carpeta '{path_ventas}' está vacía. Sube los archivos CSV correspondientes.")

# COMMAND ----------
# 2. CATÁLOGO DE PRODUCTOS (JSON)
path_productos = f"{volume_base}/landing/productos/"

if carpeta_tiene_archivos(path_productos):
    print("Ingestando Productos...")
    df_productos = (
        spark.read.format("json")
        .load(path_productos)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", lit("landing/productos"))
    )

    df_productos.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.bronze.bronze_productos")
    print("Productos ingestados correctamente.")
else:
    print(f"ADVERTENCIA: La carpeta '{path_productos}' está vacía. Sube el archivo JSON correspondientes.")

# COMMAND ----------
# 3. EMPLEADOS RR.HH. (CSV)
path_empleados = f"{volume_base}/landing/empleados/"

if carpeta_tiene_archivos(path_empleados):
    print("Ingestando Empleados...")
    df_empleados = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path_empleados)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    df_empleados.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog_name}.bronze.bronze_empleados")
    print("Empleados ingestados correctamente.")
else:
    print(f"ADVERTENCIA: La carpeta '{path_empleados}' está vacía. Sube los archivos CSV correspondientes.")

# COMMAND ----------
# 4. RESEÑAS DE CLIENTES (JSON)
path_resenas = f"{volume_base}/landing/resenas/"

if carpeta_tiene_archivos(path_resenas):
    print("Ingestando Reseñas...")
    df_resenas = (
        spark.read.format("json")
        .load(path_resenas)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    df_resenas.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog_name}.bronze.bronze_resenas")
    print("Reseñas ingestadas correctamente.")
else:
    print(f"ADVERTENCIA: La carpeta '{path_resenas}' está vacía. Sube los archivos JSON correspondientes.")

# COMMAND ----------
# 5. DEVOLUCIONES (CSV)
path_devoluciones = f"{volume_base}/landing/devoluciones/"

if carpeta_tiene_archivos(path_devoluciones):
    print("Ingestando Devoluciones...")
    df_devoluciones = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path_devoluciones)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    df_devoluciones.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog_name}.bronze.bronze_devoluciones")
    print("Devoluciones ingestadas correctamente.")
else:
    print(f"ADVERTENCIA: La carpeta '{path_devoluciones}' está vacía. Sube los archivos CSV correspondientes.")