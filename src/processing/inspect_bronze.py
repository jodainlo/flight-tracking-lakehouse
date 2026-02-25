import os
from pyspark.sql import SparkSession

# --- INICIO PARA WINDOWS (Siempre necesario en Spark) ---
hadoop_dir = os.path.join(os.getcwd(), 'hadoop')
os.environ['HADOOP_HOME'] = hadoop_dir
hadoop_bin = os.path.join(hadoop_dir, 'bin')
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
# --- FIN ---

# 1. Inicializar una Spark Session simple (no streaming, solo batch)
spark = SparkSession.builder \
    .appName("InspectorBronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Inspeccionando el Lakehouse (Capa Bronze)...")

# 2. Leer los archivos Parquet que hemos estado guardando
ruta_bronze = "data/bronze/vuelos"

try:
    df_bronze = spark.read.parquet(ruta_bronze)

    # 3. Contar cuántos registros tenemos en total
    total_vuelos = df_bronze.count()
    print(f"\nSe encontraron un total de {total_vuelos} registros de vuelo guardados en disco.")
    print("Mostrando una muestra de los ultimos 20 registros:\n")

    # 4. Mostrar la tabla (ordenada por tiempo para ver los más recientes)
    df_bronze.orderBy(df_bronze["timestamp"].desc()).show(20, truncate=False)

except Exception as e:
    print(f"\nError leyendo la capa bronze. ¿Quizas aun no se han guardado datos? Detalle: {e}")

finally:
    spark.stop()