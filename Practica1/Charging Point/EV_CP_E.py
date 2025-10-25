#Permance a la espera hasta recibir una senal de Central
#Debe comunicarse con Monitor para recibir su ID y Monitor debe comunicarse con Central para validar el ID

import socket
import sys
import threading
import time

# --- Variables Globales ---
KAFKA_ADDR = ()
ENGINE_LISTEN_ADDR = ()
simular_averia = False

def load_config(filename="engine.conf"):
    #Lee el archivo de configuración y devuelve un diccionario con los parámetros
    config = {}
    try:
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue # Ignora líneas vacías o comentarios
                
                try:
                    clave, valor = line.split('=', 1)
                    clave = clave.strip()
                    valor = valor.strip()
                    
                    # Convertir puertos a enteros
                    if 'PUERTO' in clave:
                        config[clave] = int(valor)
                    else:
                        config[clave] = valor
                except ValueError:
                    print(f"[WARN] Línea mal formada en {filename}: {line}")
        
        print(f"[INFO] Configuración cargada desde {filename}")
        return config

    except FileNotFoundError:
        print(f"[ERROR-FATAL] No se encontró el archivo de configuración: {filename}")
        return None
    except Exception as e:
        print(f"[ERROR-FATAL] No se pudo leer el archivo de configuración: {e}")
        return None

def start_monitor_server():
    """
    Inicia el servidor que escucha al Monitor.
    Usa la IP/Puerto cargados desde 'engine.conf'.
    """
    global ENGINE_LISTEN_ADDR
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # USA LA VARIABLE GLOBAL CARGADA DESDE EL FICHERO
            s.bind(ENGINE_LISTEN_ADDR) 
            s.listen()
            print(f"[INFO] Servidor del Monitor escuchando en {ENGINE_LISTEN_ADDR}...")

            while True:
                conn, addr = s.accept()
                client_thread = threading.Thread(
                    target=handle_monitor_connection, 
                    args=(conn, addr),
                    daemon=True
                )
                client_thread.start()
                
    except Exception as e:
        print(f"[ERROR-FATAL] El servidor del Monitor ha fallado: {e}")

def handle_monitor_connection(conn, addr):
    #Gestiona cada conexión del Monitor en un hilo separado.
    #Responde OK o KO al HEALTH_CHECK.
    global simular_averia
    print(f"[OK] Monitor conectado desde {addr}")
    
    try:
        while True:
            mensaje = conn.recv(1024).decode('utf-8')
            if not mensaje:
                print(f"[INFO] Monitor {addr} desconectado.")
                break
            
            if "HEALTH_CHECK" in mensaje:
                if simular_averia:
                    conn.sendall(b"KO: Falla simulada")
                else:
                    conn.sendall(b"OK")
            else:
                print(f"[WARN] Mensaje inesperado del Monitor: {mensaje}")
                
    except Exception as e:
        print(f"[ERROR] Error con el Monitor {addr}: {e}")
    finally:
        conn.close()

def failure_simulation_thread():
    #Hilo que espera 'a' (avería) o 'r' (resolver) por teclado.
    global simular_averia
    print("\n--- [SIMULADOR DE AVERÍAS] ---")
    print("Pulsa 'a' y 'Enter' para SIMULAR una avería.")
    print("Pulsa 'r' y 'Enter' para RESOLVER una avería.")
    print("---------------------------------\n")
    
    while True:
        try:
            comando = input()
            if comando.strip().lower() == 'a':
                simular_averia = True
                print("\n[!!!] AVERÍA SIMULADA. Engine responderá KO.\n")
            elif comando.strip().lower() == 'r':
                simular_averia = False
                print("\n[OK] AVERÍA RESUELTA. Engine responderá OK.\n")
        except EOFError:
            pass

def __main__():
    global KAFKA_ADDR, ENGINE_LISTEN_ADDR

    if len(sys.argv) != 3:
        print("Uso: python EV_CP_E.py <IP_KAFKA> <PUERTO_KAFKA>")
        return
    
    KAFKA_ADDR = (sys.argv[1], int(sys.argv[2]))

    # 1. Cargar la configuración desde el archivo
    config = load_config("engine.conf")
    if config is None:
        return # Si falla la carga, el programa termina

    # 2. Asignar las variables globales desde la configuración
    try:
        ENGINE_LISTEN_ADDR = (config['IP_ESCUCHA'], config['PUERTO_ESCUCHA'])
    except KeyError as e:
        print(f"[ERROR-FATAL] Falta una clave en el archivo de configuración: {e}")
        return

    print(f"[INFO] Engine iniciado.")
    print(f"[INFO] Conectará a Kafka en {KAFKA_ADDR}")
    print(f"[INFO] Escuchando al Monitor en {ENGINE_LISTEN_ADDR}")
    
    # Hilo 1: Escucha al Monitor
    monitor_server_thread = threading.Thread(
        target=start_monitor_server, 
        daemon=True
    )
    
    # Hilo 2: Simula averías por teclado
    failure_sim_thread = threading.Thread(
        target=failure_simulation_thread, 
        daemon=False 
    )

    monitor_server_thread.start()
    failure_sim_thread.start()

    failure_sim_thread.join()
    print("\n[INFO] Cerrando Engine...")

if __name__ == "__main__":
    __main__()