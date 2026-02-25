import os
import time
import json
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

# 1. Cargar las nuevas Llaves Maestras
load_dotenv()
CLIENT_ID = os.getenv('OPENSKY_CLIENT_ID')
CLIENT_SECRET = os.getenv('OPENSKY_CLIENT_SECRET')

# 2. Bounding Box: Corredor Andino
BBOX = {
    'lamin': -55.0, # Límite Sur (Patagonia)
    'lamax': 13.0,  # Límite Norte (Mar Caribe)
    'lomin': -85.0, # Límite Oeste (Océano Pacífico)
    'lomax': -65.0  # Límite Este (Amazonía profunda)
}

# 3. Configurar la sala de espera (Redpanda)
conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'radar-aviones'
}
producer = Producer(conf)

def obtener_token():
    """Esta funcion viaja a OpenSky y cambia las llaves por un Token de acceso"""
    url_token = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    try:
        response = requests.post(url_token, data=payload)
        if response.status_code == 200:
            return response.json().get('access_token')
        else:
            print(f"Error obteniendo token: {response.status_code}")
            print(f"Detalle del rechazo: {response.text}")
            return None
    except Exception as e:
        print(f"Error de red al pedir token: {e}")
        return None

def fetch_and_produce():
    url_api = "https://opensky-network.org/api/states/all"
    
    print("Autenticando con OpenSky de forma segura...")
    token = obtener_token()
    
    if not token:
        print("No se pudo obtener el token. Revisa tu archivo .env")
        return

    # Ponemos el Token en la "cabecera" de nuestra petición
    headers = {
        'Authorization': f'Bearer {token}'
    }

    print("Iniciando radar en tiempo real (Presiona Ctrl+C para detener)...")
    
    while True:
        try:
            response = requests.get(url_api, headers=headers, timeout=10)
            
            # Si el Token caduca, el script pide uno nuevo solito y vuelve a intentar
            if response.status_code == 401:
                print("Token expirado. Renovando automaticamente...")
                token = obtener_token()
                headers['Authorization'] = f'Bearer {token}'
                continue

            if response.status_code == 200:
                data = response.json()
                states = data.get('states')
                
                if states:
                    hora_actual = time.strftime('%H:%M:%S')
                    print(f"[{hora_actual}] Enviando {len(states)} aviones REALES a Redpanda...")
                    
                    for avion in states:
                        vuelo = {
                            "id_vuelo": avion[0],
                            "callsign": avion[1].strip() if avion[1] else "Desconocido",
                            "pais": avion[2],
                            "longitud": avion[5],
                            "latitud": avion[6],
                            "altitud": avion[7],
                            "velocidad": avion[9],
                            "timestamp": int(time.time())
                        }
                        producer.produce('vuelos-andinos', value=json.dumps(vuelo).encode('utf-8'))
                    producer.flush()
                else:
                    print(f"[{time.strftime('%H:%M:%S')}] Cielo despejado. No hay aviones.")
            else:
                print(f"Error API: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"Error de red: {e}")
            
        # Esperar 60 segundos antes de la siguiente consulta
        time.sleep(30)

if __name__ == "__main__":
    fetch_and_produce()