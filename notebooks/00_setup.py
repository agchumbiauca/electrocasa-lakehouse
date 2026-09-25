# Databricks notebook source

import os

catalog_name = "catalogo_electrocasa"
volume_path = f"/Volumes/{catalog_name}/default/datos_proyecto"

# 1. Crear Schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze;")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver;")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold;")

# 2. Crear carpetas en el Volumen (POSX / Python standard)
subfolders = [
    "landing/ventas", "landing/productos", "landing/empleados",
    "landing/resenas", "landing/devoluciones", "checkpoints"
]
for subfolder in subfolders:
    os.makedirs(f"{volume_path}/{subfolder}", exist_ok=True)

# 3. Asignación de permisos por Grupos
grupos_permisos = [
    ("bronze", "grupo_ingenieria", "ALL PRIVILEGES"),
    ("silver", "grupo_ingenieria", "ALL PRIVILEGES"),
    ("gold", "grupo_ingenieria", "ALL PRIVILEGES"),
    ("gold", "grupo_analistas", "SELECT"),
    ("gold", "grupo_auditoria", "SELECT")
]

for schema, grupo, privilegio in grupos_permisos:
    try:
        spark.sql(f"GRANT {privilegio} ON SCHEMA {catalog_name}.{schema} TO `{grupo}`;")
    except Exception as e:
        print(f"Error asignando permisos al grupo {grupo}: {e}")