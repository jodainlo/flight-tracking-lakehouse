import os
from pyspark.sql import SparkSession

# --- INICIO PARA WINDOWS ---
hadoop_dir = os.path.join(os.getcwd(), 'hadoop')
os.environ['HADOOP_HOME'] = hadoop_dir
hadoop_bin = os.path.join(hadoop_dir, 'bin')
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
# --- FIN ---

print("Iniciando conexion con la Base de Datos PostgreSQL...")

# 1. Inicializar Spark y descargar el Driver de PostgreSQL automáticamente
spark = SparkSession.builder \
    .appName("ExportarAPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Leer la Capa Gold (El origen)
ruta_gold = "data/gold/vuelos_resumen"
try:
    df_gold = spark.read.parquet(ruta_gold)
    print(f"Capa Gold leida correctamente. Total de aviones a exportar: {df_gold.count()}")
except Exception as e:
    print(f"Error leyendo la Capa Gold. Detalle: {e}")
    spark.stop()
    exit()

# 3. Configurar las credenciales (El destino: nuestro contenedor Docker)
postgres_url = "jdbc:postgresql://localhost:5433/vuelos_db"
postgres_properties = {
    "user": "postgres",
    "password": "12345678",
    "driver": "org.postgresql.Driver"
}

# 4. Inyectar los datos en PostgreSQL
print("Escribiendo datos en la tabla 'vuelos_gold' de PostgreSQL...")

df_gold.write \
    .jdbc(url=postgres_url, 
          table="vuelos_gold", 
          mode="overwrite", 
          properties=postgres_properties)

print("¡EXITO! La Capa Gold ha sido inyectada en la base de datos corporativa.")

spark.stop()