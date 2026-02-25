import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import from_json, col

# --- INICIO PARA WINDOWS ---
hadoop_dir = os.path.join(os.getcwd(), 'hadoop')
os.environ['HADOOP_HOME'] = hadoop_dir

# Forzamos a Windows a leer el archivo hadoop.dll agregando la carpeta 'bin' al PATH
hadoop_bin = os.path.join(hadoop_dir, 'bin')
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
# --- FIN ---

# 1. Inicializar Spark Session
spark = SparkSession.builder \
    .appName("ProcesadorVuelosAndinos") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Motor de Spark encendido. Conectando a la sala de espera (Redpanda)...")

# 2. Definir el Esquema (La estructura de nuestra futura tabla)
esquema_vuelo = StructType([
    StructField("id_vuelo", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("longitud", DoubleType(), True),
    StructField("latitud", DoubleType(), True),
    StructField("altitud", DoubleType(), True),
    StructField("velocidad", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# 3. Leer el stream crudo desde Redpanda
df_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "vuelos-andinos") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transformación: De binario -> a JSON String -> a Columnas Estructuradas
df_estructurado = df_raw \
    .selectExpr("CAST(value AS STRING) as json_crudo") \
    .withColumn("datos_vuelo", from_json(col("json_crudo"), esquema_vuelo)) \
    .select("datos_vuelo.*") # El asterisco expande todos los campos en columnas

# 5. Escribir el resultado en el Lakehouse (Capa Bronze - Formato Parquet)
print("Guardando vuelos estructurados en el Lakehouse (data/bronze/vuelos)...")
query = df_estructurado \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/bronze/vuelos") \
    .option("checkpointLocation", "data/checkpoints/vuelos_bronze") \
    .start()

query.awaitTermination()