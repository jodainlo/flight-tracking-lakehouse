import os
from pyspark.sql import SparkSession

# --- INICIO PARA WINDOWS ---
hadoop_dir = os.path.join(os.getcwd(), 'hadoop')
os.environ['HADOOP_HOME'] = hadoop_dir
hadoop_bin = os.path.join(hadoop_dir, 'bin')
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
# --- FIN ---

# 1. Inicializar Spark Session
spark = SparkSession.builder \
    .appName("AnalisisSQLSilver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Iniciando Motor Spark SQL sobre la Capa Silver...")

# 2. Leer la Capa Silver
ruta_silver = "data/silver/vuelos"
df_silver = spark.read.parquet(ruta_silver)

# 3. Convertir el DataFrame en una tabla SQL temporal
df_silver.createOrReplaceTempView("vuelos")

# --- CONSULTA 1: Top Países con más tráfico ---
print("\n1. Top 5 paises con mas trafico aereo en la region:")
query_paises = spark.sql("""
    SELECT pais, COUNT(*) as total_vuelos 
    FROM vuelos 
    GROUP BY pais 
    ORDER BY total_vuelos DESC 
    LIMIT 5
""")
query_paises.show()

# --- CONSULTA 2: Los aviones más altos ---
print("\n2. Los 5 aviones volando a mayor altitud (metros):")
query_altitud = spark.sql("""
    SELECT callsign, pais, altitud, velocidad 
    FROM vuelos 
    ORDER BY altitud DESC 
    LIMIT 5
""")
query_altitud.show()

# --- CONSULTA 3: Velocidad promedio por país ---
print("\n3. Velocidad promedio de los aviones por pais (solo paises con mas de 5 vuelos):")
query_velocidad = spark.sql("""
    SELECT pais, ROUND(AVG(velocidad), 2) as velocidad_promedio_ms 
    FROM vuelos 
    GROUP BY pais 
    HAVING COUNT(*) > 5
    ORDER BY velocidad_promedio_ms DESC
""")
query_velocidad.show()

spark.stop()