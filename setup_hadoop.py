import os
import urllib.request

# Crea las carpetas necesarias en el proyecto
os.makedirs('hadoop/bin', exist_ok=True)
base_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/"

print("Descargando winutils.exe...")
urllib.request.urlretrieve(base_url + "winutils.exe", "hadoop/bin/winutils.exe")

print("Descargando hadoop.dll...")
urllib.request.urlretrieve(base_url + "hadoop.dll", "hadoop/bin/hadoop.dll")

print("Completado! Archivos de Hadoop listos.")