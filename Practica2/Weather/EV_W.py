# ========================================================================== #
# Debe consultar el clima cada 4 segundos y avisar a la Central si hace 
# demasiado frío (por debajo de 0ºC). Cuando vuelva a no hacer tanto frío
# se vuelve a notificar a Central.
# ========================================================================== #

import requests
import time
import sys

# ----------------- #
# Configuration
# ----------------- #
API_KEY = "ec775469612f2a4b0ef69d577a671f72"
CITIES = ["Alicante"]
CENTRAL_API_URL = ""
FREEZE = 4 #Seconds

def get_temperature(city):
    """
    Consulta la temperatura actual de algun lugar
    """
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            
            temp_kelvin = data['main']['temp']
            temp_celsius = temp_kelvin - 273.15
            return temp_celsius
        else:
            print(f"[ERROR] No se pudo obtener clima para {city}: {response.status_code}")
            return None
    except Exception as e:
        print(f"[ERROR] Fallo de conexión con OpenWeather: {e}")
        return None
    
def main():
    city = CITIES[0]
    print(f"Temperatura en {city}: {get_temperature(city):.1f}")
    
if __name__ == "__main__":
    main()