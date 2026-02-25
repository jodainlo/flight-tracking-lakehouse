import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, avg, count, round, last

# --- INICIO PARA WINDOWS ---
hadoop_dir = os.path.join(os.getcwd(), 'hadoop')
os.environ['HADOOP_HOME'] = hadoop_dir
hadoop_bin = os.path.join(hadoop_dir, 'bin')
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
# --- FIN ---

spark = SparkSession.builder.appName("CapaGoldVuelos").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Iniciando agregacion hacia la Capa Gold (Business Level)...")

ruta_silver = "data/silver/vuelos"
df_silver = spark.read.parquet(ruta_silver)

# Agregamos ultima_latitud y ultima_longitud para nuestro mapa
df_gold = df_silver.groupBy("id_vuelo", "callsign", "pais") \
    .agg(
        max("altitud").alias("altitud_maxima_m"),
        round(avg("velocidad"), 2).alias("vel_promedio_ms"),
        count("*").alias("veces_visto_por_radar"),
        max("timestamp").alias("ultimo_avistamiento"),
        last("latitud").alias("ultima_latitud"),     
        last("longitud").alias("ultima_longitud")   
    )

ruta_gold = "data/gold/vuelos_resumen"

df_gold.write.mode("overwrite").parquet(ruta_gold)

print(f"¡Capa Gold actualizada con coordenadas para el mapa!\n")
spark.stop()