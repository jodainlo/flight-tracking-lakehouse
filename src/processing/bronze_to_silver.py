import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- INICIO PARA WINDOWS ---
hadoop_dir = os.path.join(os.getcwd(), 'hadoop')
os.environ['HADOOP_HOME'] = hadoop_dir
hadoop_bin = os.path.join(hadoop_dir, 'bin')
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
# --- FIN ---

# 1. Inicializar Spark Session (Modo Batch)
spark = SparkSession.builder \
    .appName("CapaSilverVuelos") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Iniciando transformacion hacia la Capa Silver...")

# 2. Leer la Capa Bronze
ruta_bronze = "data/bronze/vuelos"
df_bronze = spark.read.parquet(ruta_bronze)

total_raw = df_bronze.count()
print(f"Total de registros crudos en Bronze: {total_raw}")

# 3. Limpieza y Transformación
# - dropna: Elimina filas donde falten las coordenadas o la velocidad
# - filter: Nos quedamos solo con aviones que tengan altitud y velocidad mayor a 0 (están volando)
# - dropDuplicates: Evitamos tener el mismo avión duplicado en el mismo segundo exacto
df_silver = df_bronze \
    .dropna(subset=["latitud", "longitud", "altitud", "velocidad"]) \
    .filter((col("altitud") > 0) & (col("velocidad") > 0)) \
    .dropDuplicates(["id_vuelo", "timestamp"])

total_clean = df_silver.count()
print(f"Total de vuelos limpios y en el aire (Silver): {total_clean}")
print(f"Registros descartados (en tierra, duplicados o errores de sensor): {total_raw - total_clean}")

# 4. Guardar en la Capa Silver (Formato Parquet)
# Usamos mode("overwrite") para que cada vez que ejecutemos el script, 
# reescriba la carpeta con la información más limpia y actualizada.
ruta_silver = "data/silver/vuelos"

df_silver.write \
    .mode("overwrite") \
    .parquet(ruta_silver)

print(f"¡Datos procesados y guardados exitosamente en {ruta_silver}!")

# Imprimimos una muestra de los datos limpios
print("\nMuestra de la Capa Silver (Sin NULLs y solo aviones volando):")
df_silver.orderBy(col("timestamp").desc()).show(10, truncate=False)

spark.stop()