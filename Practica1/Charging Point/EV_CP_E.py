# --- Imports ---
import socket
import sys
import threading
import json
import time
from kafka import KafkaProducer, KafkaConsumer

# --- Variables Globales ---
KAFKA_ADDR = ()
ENGINE_LISTEN_ADDR = ()
simular_averia = False
kafka_producer = None
cp_id = "CP_SIN_ID" 
charge_request_pending = False
precio_kwh = 0.0


# Diccionario para manejar el estado de la carga actual
current_charge = {
    "active": False,
    "driver_id": None,
    "start_time": None,
    "kwh_consumed": 0.0
}

def create_kafka_producer(kafka_ip_port_str):
    global kafka_producer
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_ip_port_str,
            api_version=(4, 1, 0),
            # Le decimos cómo "escribir" los mensajes (convertir de diccionario a JSON)
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"[INFO] Productor de Kafka conectado a {kafka_ip_port_str}")
    except Exception as e:
        print(f"[ERROR-FATAL] No se pudo conectar el productor de Kafka: {e}")
        kafka_producer = None


def start_supply_simulation_thread():

    #Hilo que simula la carga y envía telemetría CADA SEGUNDO.
    #Este es el "Productor" principal.

    global current_charge, kafka_producer, cp_id, precio_kwh
    
    while True:
        # Solo hacemos algo si la carga está activa ("e" pulsado)
        # y si el productor de Kafka está conectado.
        if current_charge["active"] and kafka_producer:
            
            # 1. Simular el paso de 1 segundo de carga
            # (Vamos a simular que carga 0.01 KWh cada segundo)
            current_charge["kwh_consumed"] += precio_kwh/3600
            
            # 2. Calcular los valores actuales
            kwh = round(current_charge['kwh_consumed'], 2)
            cost = round(kwh * precio_kwh, 2)
            driver = current_charge['driver_id']
            
            # 3. Construir el mensaje de telemetría 
            telemetry_message = {
                "cp_id": cp_id,
                "status": "SUMINISTRANDO",
                "driver_id": driver,
                "kwh_consumed": kwh,  # Consumo en Kw (en este caso, KWh acumulados) [cite: 87]
                "cost_euros": cost    # Importe en € [cite: 88]
            }
            
            # 4. Enviar a Kafka al topic de telemetría
            try:
                # print(f"DEBUG: Enviando telemetría: {kwh} KWh") # (Descomentar para depurar)
                kafka_producer.send('telemetry_from_cp', telemetry_message)
            except Exception as e:
                print(f"[ERROR-KAFKA] No se pudo enviar telemetría: {e}")

            time.sleep(1) # ¡Esperamos 1 segundo! 
        
        else:
            # Si no está cargando, dormimos 1 seg para no saturar la CPU
            time.sleep(1)

def user_interface_thread():
    
    #Hilo que maneja TODAS las entradas de teclado del usuario.
    #'a'/'r' para averías.
    #'e'/'d' para simular enchufe/desenchufe.
    
    global simular_averia, charge_request_pending, current_charge, kafka_producer, cp_id, precio_kwh
    
    print("\n--- [CONSOLA DE SIMULACIÓN DEL CP] ---")
    print("Comandos disponibles en cualquier momento:")
    print("  'a' y 'Enter' -> SIMULAR una avería.")
    print("  'r' y 'Enter' -> RESOLVER una avería.")
    print("Comandos de carga (cuando se reciba una solicitud):")
    print("  'e' y 'Enter' -> ENCHUFAR vehículo (inicia suministro)")
    print("  'd' y 'Enter' -> DESENCHUFAR vehículo (detiene suministro)")
    print("---------------------------------------\n")

    while True:
        try:
            comando = input()
            if comando.strip().lower() == 'a':
                simular_averia = True
                print("\n[!!!] AVERÍA SIMULADA. Engine responderá KO.\n")

            elif comando.strip().lower() == 'r':
                simular_averia = False
                print("\n[OK] AVERÍA RESUELTA. Engine responderá OK.\n")

            elif comando.strip().lower() == 'e':

                if charge_request_pending:
                    charge_request_pending = False
                    current_charge["active"] = True
                    # Actualizamos la hora de inicio al momento real del enchufe
                    current_charge["start_time"] = time.time() 
                    print(f"\n[>>>] VEHÍCULO ENCHUFADO. Iniciando suministro para {current_charge['driver_id']}...\n")
                else:
                    print("\n[!] No hay ninguna solicitud de carga pendiente. 'e' no hace nada.\n")

            elif comando.strip().lower() == 'd':

                if current_charge["active"]:
                    print(f"\n[<<<] VEHÍCULO DESENCHUFADO. Deteniendo suministro para {current_charge['driver_id']}.\n")
                    
                    # --- Enviar Ticket Final ---
                    # Al desenchufar, enviamos el "ticket" final a CENTRAL [cite: 178]
                    kwh = round(current_charge['kwh_consumed'], 2)
                    cost = round(kwh * precio_kwh, 2)
                    
                    ticket_message = {
                        "cp_id": cp_id,
                        "status": "TICKET_FINAL",
                        "driver_id": current_charge['driver_id'],
                        "kwh_total": kwh,
                        "cost_total": cost
                    }
                    #Envío del ticket
                    try:
                        if kafka_producer:
                            kafka_producer.send('telemetry_from_cp', ticket_message)
                            print(f"[INFO] Ticket final enviado a CENTRAL: {kwh} KWh, {cost} €")
                    except Exception as e:
                        print(f"[ERROR] No se pudo enviar el ticket final: {e}")

                    current_charge["active"] = False
                    current_charge["driver_id"] = None
                else:
                    print("\n[!] No hay ningún vehículo enchufado. 'd' no hace nada.\n")

        except EOFError:
            pass

def start_kafka_consumer_thread():
    
    #Hilo que espera órdenes de Central

    global KAFKA_ADDR, current_charge, cp_id
    
    # Preparamos la dirección del servidor de Kafka
    kafka_ip_port_str = f"{KAFKA_ADDR[0]}:{KAFKA_ADDR[1]}"
    
    print(f"[INFO] Hilo consumidor iniciándose...")

    try:
        consumer = KafkaConsumer(
            'commands_to_cp', # <-- Topic de Kafka
            bootstrap_servers=kafka_ip_port_str, # <-- La dirección del servidor Kafka
            auto_offset_reset='latest', # <-- Solo los mensajes nuevos
            api_version=(4, 1, 0),
            
            # Kafka envía texto raro. Esto le dice cómo convertir ese texto
            # en un diccionario Python que podamos entender (usando json).
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"[INFO] Consumidor de Kafka escuchando en ' xd '...")
        for message in consumer:
            data = message.value
            print(f"[KAFKA-RX] Mensaje recibido: {data}")

            #Si el mensaje no es para este CP
            if data.get("target_cp") != cp_id:
                print(f"[KAFKA-RX] Mensaje ignorado, no es para mí ({cp_id})")
                continue #Vuelve a la espera del mensaje

            #En caso de que si lo sea, leemos el comando para ejecutarlo
            command = data.get("command")

            if command == "START":
                # Comprobamos si NO estamos cargando ya
                if not current_charge["active"] and not charge_request_pending:
                    driver = data.get("driver_id", "DriverDesconocido")
                    
                    current_charge["driver_id"] = driver
                    current_charge["start_time"] = time.time() 
                    current_charge["kwh_consumed"] = 0.0

                    # Cambiamos el estado
                    charge_request_pending = True

                    print(f"\n[!!!] SOLICITUD DE CARGA RECIBIDA para {driver} [!!!]")
                    print(f"[..._] Pulse 'e' para ENCHUFAR el vehículo.")

                else:
                    print(f"[WARN] Se recibió START, pero ya estaba cargando.")

            elif command == "STOP":
                if current_charge["active"] or charge_request_pending:
                    print(f"\n[!!!] FIN DE CARGA (orden central) para {current_charge['driver_id']} [!!!]\n")
                    current_charge["active"] = False
                    charge_request_pending = False
                    current_charge["driver_id"] = None                   

                else:
                    print(f"[WARN] Se recibió STOP, pero no estaba cargando.")
        
    except Exception as e:
        print(f"[ERROR-FATAL] El consumidor de Kafka ha fallado: {e}")

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






 ###################################
#
# MAIN
#       
 ###################################

def __main__():
    global KAFKA_ADDR, ENGINE_LISTEN_ADDR, cp_id, precio_kwh, kafka_producer # <-- Añade las globales

    # 1. Leer argumentos de Kafka
    if len(sys.argv) != 3:
        print("Uso: python EV_CP_E.py <IP_KAFKA> <PUERTO_KAFKA>")
        return
    
    KAFKA_ADDR = (sys.argv[1], int(sys.argv[2]))
    kafka_ip_port_str = f"{KAFKA_ADDR[0]}:{KAFKA_ADDR[1]}" # String para Kafka

    # 2. Cargar la configuración desde el archivo
    config = load_config("engine.conf")
    if config is None:
        return 

    # 3. Asignar las variables globales desde la configuración
    try:
        ENGINE_LISTEN_ADDR = (config['IP_ESCUCHA'], config['PUERTO_ESCUCHA'])
        cp_id = config['CP_ID'] # <-- ¡IMPORTANTE!
        precio_kwh = float(config['PRECIO_KWH']) # <-- ¡IMPORTANTE!
    except KeyError as e:
        print(f"[ERROR-FATAL] Falta una clave en el archivo de configuración: {e}")
        return
    except ValueError:
        print(f"[ERROR-FATAL] El PRECIO_KWH en engine.conf no es un número válido.")
        return

    print(f"[INFO] Engine iniciado para CP_ID: {cp_id} (Precio: {precio_kwh} €/KWh)")
    print(f"[INFO] Conectará a Kafka en {KAFKA_ADDR}")
    print(f"[INFO] Escuchando al Monitor en {ENGINE_LISTEN_ADDR}")
    
    # 4. Inicializar el productor de Kafka
    create_kafka_producer(kafka_ip_port_str)
    if kafka_producer is None:
        print("[ERROR-FATAL] Saliendo porque el productor de Kafka no pudo iniciarse.")
        return 

    # --- Creación de todos los Hilos ---
    
    # Hilo 1: Escucha al Monitor (Socket)
    monitor_server_thread = threading.Thread(
        target=start_monitor_server, 
        daemon=True
    )
    
    # Hilo 2: Interfaz de Usuario
    ui_thread = threading.Thread(
        target=user_interface_thread, 
        daemon=False 
    )

    # Hilo 3: Escucha comandos de CENTRAL (Kafka Consumer)
    kafka_consumer_thread = threading.Thread(
        target=start_kafka_consumer_thread,
        daemon=True
    )
    
    # Hilo 4: Simula la carga y envía telemetría (Kafka Producer)
    supply_sim_thread = threading.Thread(
        target=start_supply_simulation_thread,
        daemon=True
    )

    # --- Lanzar todos los hilos ---
    monitor_server_thread.start()
    kafka_consumer_thread.start()
    supply_sim_thread.start()
    ui_thread.start() 

    # --- Espera y Limpieza ---
    ui_thread.join()
    
    if kafka_producer:
        kafka_producer.close() # Limpiar conexión de Kafka al salir
    print("\n[INFO] Cerrando Engine...")

if __name__ == "__main__":
    __main__()