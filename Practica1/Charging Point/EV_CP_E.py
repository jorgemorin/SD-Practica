# --- Imports ---
import socket
import sys
import threading
import json
import time
from kafka import KafkaProducer, KafkaConsumer
import os # NUEVO: Para limpiar la pantalla
from io import StringIO # NUEVO: Para capturar prints

# --- 0. VISUALS (NUEVO) ---
if os.name == 'nt':
    os.system('color') # Habilitar colores en Windows

def _clear_screen():
    """Limpia la pantalla de la terminal."""
    os.system('cls' if os.name == 'nt' else 'clear')

class Colors:
    """Clase para colores de terminal ANSI."""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'

# --- 1. Variables Globales (Lógica) ---
# (Sin cambios, 100% original)
KAFKA_ADDR = ()
ENGINE_LISTEN_ADDR = ()
simular_averia = False
kafka_producer = None
cp_id = "CP_SIN_ID" 
charge_request_pending = False
precio_kwh = 0.0

current_charge = {
    "active": False,
    "driver_id": None,
    "start_time": None,
    "kwh_consumed": 0.0
}

# --- 2. Globales para la UI (NUEVO) ---
UI_LOCK = threading.Lock()
UI_KAFKA_ADDR = "N/A"
UI_MONITOR_ADDR = "N/A"
UI_LAST_LOGS = [] # Una lista de los últimos logs
UI_MONITOR_CLIENT = "N/A"
_original_print = print # Guardamos la función print original

def _add_log(message: str):
    """Añade un mensaje al log de la UI de forma segura."""
    with UI_LOCK:
        # Añadir con timestamp simple
        log_entry = f"{time.strftime('%H:%M:%S')} {message}"
        UI_LAST_LOGS.append(log_entry)
        # Mantener solo los últimos 5 mensajes
        if len(UI_LAST_LOGS) > 5:
            UI_LAST_LOGS.pop(0)

def hijacked_print(*args, **kwargs):
    """
    NUEVO: Esta función reemplaza a 'print'.
    En lugar de imprimir en la consola, añade el mensaje al log de la UI.
    """
    # Convertir los argumentos de print a un string
    output_buffer = StringIO()
    _original_print(*args, file=output_buffer, **kwargs)
    message = output_buffer.getvalue().strip()
    output_buffer.close()
    
    # No loguear strings vacíos
    if message:
        _add_log(message)

# Sobrescribimos la función print global
print = hijacked_print


# --- 4. Lógica de Negocio (Sin cambios, solo los prints se redirigen) ---

def create_kafka_producer(kafka_ip_port_str):
    global kafka_producer
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_ip_port_str,
            api_version=(4, 1, 0),
            value_serializer=lambda v: v.encode('utf-8')
        )
        # Este print ahora va al log de la UI
        _original_print(f"{Colors.GREEN}[INFO] Productor de Kafka conectado a {kafka_ip_port_str}{Colors.RESET}")
    except Exception as e:
        _original_print(f"{Colors.RED}[ERROR-FATAL] No se pudo conectar el productor de Kafka: {e}{Colors.RESET}")
        kafka_producer = None


def start_supply_simulation_thread():
    # LÓGICA 100% ORIGINAL
    global current_charge, kafka_producer, cp_id, precio_kwh
    
    while True:
        if current_charge["active"] and kafka_producer:
            current_charge["kwh_consumed"] += 0.01
            kwh = round(current_charge['kwh_consumed'], 2)
            cost = round(kwh * precio_kwh, 2)
            driver = current_charge['driver_id']
            telemetry_message = f"SUMINISTRANDO:{cp_id}:{kwh}:{cost}"
            try:
                kafka_producer.send('CPTelemetry', telemetry_message)
            except Exception as e:
                print(f"{Colors.RED}[ERROR-KAFKA] No se pudo enviar telemetría: {e}{Colors.RESET}")

            time.sleep(1) 
        else:
            time.sleep(1)

def user_interface_thread():
    """
    MODIFICADO: Este hilo ahora es el BUCLE DE RENDERIZADO de la UI.
    Sigue manejando el input, pero ahora también dibuja la pantalla.
    """
    global simular_averia, charge_request_pending, current_charge, kafka_producer, cp_id, precio_kwh
    
    # Imprimir la ayuda una vez (no se refresca)
    _clear_screen()
    _original_print(f"{Colors.CYAN}--- [CONSOLA DE SIMULACIÓN DEL CP] ---{Colors.RESET}")
    _original_print("Comandos disponibles:")
    _original_print(f"  {Colors.YELLOW}'a'{Colors.RESET} -> SIMULAR una avería.")
    _original_print(f"  {Colors.YELLOW}'r'{Colors.RESET} -> RESOLVER una avería.")
    _original_print(f"  {Colors.YELLOW}'e'{Colors.RESET} -> ENCHUFAR vehículo (si hay solicitud).")
    _original_print(f"  {Colors.YELLOW}'d'{Colors.RESET} -> DESENCHUFAR vehículo (detiene suministro).")
    _original_print(f"{Colors.CYAN}---------------------------------------{Colors.RESET}")
    time.sleep(1) # Pausa para leer la ayuda
    
    while True:
        try:
            # --- NUEVA SECCIÓN DE DIBUJADO ---
            _clear_screen()
            
            # 1. Leer estado global de forma segura
            with UI_LOCK:
                # Leemos las variables globales originales que tu lógica usa
                is_active = current_charge["active"]
                is_pending = charge_request_pending
                driver = current_charge["driver_id"]
                kwh = round(current_charge["kwh_consumed"], 2)
                cost = round(kwh * precio_kwh, 2)
                is_averia = simular_averia
                monitor_client = UI_MONITOR_CLIENT
                logs = list(UI_LAST_LOGS) # Copiar la lista de logs
            
            # 2. Dibujar la Interfaz
            _original_print(f"{Colors.CYAN}{Colors.BOLD}================================================{Colors.RESET}")
            _original_print(f"{Colors.CYAN}{Colors.BOLD}  EV CHARGING POINT ENGINE (CP ID: {cp_id}) {Colors.RESET}")
            _original_print(f"{Colors.CYAN}{Colors.BOLD}================================================{Colors.RESET}")
            _original_print(f"{Colors.BOLD} 🔌 Kafka:   {Colors.RESET}{UI_KAFKA_ADDR}")
            _original_print(f"{Colors.BOLD} 👁️ Monitor: {Colors.RESET}{UI_MONITOR_ADDR} (Precio: {precio_kwh} €/KWh)")
            _original_print(f"{Colors.BOLD}   └─Client: {Colors.RESET}{monitor_client}")
            _original_print("------------------------------------------------")

            # --- Panel de Estado ---
            _original_print(f"{Colors.BOLD} ESTADO CP:    {Colors.RESET}", end="")
            if is_active:
                _original_print(f"{Colors.GREEN}CARGANDO (Driver: {driver}){Colors.RESET}")
            elif is_pending:
                _original_print(f"{Colors.YELLOW}SOLICITUD PENDIENTE (Driver: {driver}){Colors.RESET}")
                # ¡Notificación clave que pediste!
                _original_print(f"\n{Colors.BOLD}{Colors.YELLOW}  --> ¡PULSA 'e' PARA ENCHUFAR! <--{Colors.RESET}")
            else:
                _original_print(f"{Colors.CYAN}ESPERANDO...{Colors.RESET}")

            # --- Panel de Avería ---
            _original_print(f"{Colors.BOLD} ESTADO AVERÍA: {Colors.RESET}", end="")
            if not is_averia:
                _original_print(f"{Colors.GREEN}OK{Colors.RESET}")
            else:
                _original_print(f"{Colors.RED}AVERÍA SIMULADA{Colors.RESET}")

            # --- Panel de Telemetría ---
            _original_print("------------------------------------------------")
            _original_print(f"{Colors.BOLD} > Consumo: {Colors.CYAN}{kwh} KWh{Colors.RESET}")
            _original_print(f"{Colors.BOLD} > Importe: {Colors.YELLOW}{cost} ${Colors.RESET}")
            _original_print("------------------------------------------------")
            
            # --- Panel de Log ---
            _original_print(f"{Colors.BOLD}LOG:{Colors.RESET}")
            for log_line in logs:
                _original_print(f"  {log_line}")

            # --- Menú de Input ---
            _original_print("------------------------------------------------")
            _original_print(f"{Colors.BOLD}[a] Avería | [r] Resolver | [e] Enchufar | [d] Desenchufar{Colors.RESET}")
            
            # --- FIN SECCIÓN DE DIBUJADO ---

            # Lógica de input (casi original)
            comando = input(f"\n{Colors.BOLD}> {Colors.RESET}")
            
            # LÓGICA 100% ORIGINAL
            if comando.strip().lower() == 'a':
                simular_averia = True
                print(f"{Colors.RED}{Colors.BOLD}\n[!!!] AVERÍA SIMULADA. Engine responderá KO.{Colors.RESET}\n")
                if current_charge["active"]:
                    print(f"{Colors.RED}\n[!!!] SUMINISTRO INTERRUMPIDO POR AVERÍA para {current_charge['driver_id']}.{Colors.RESET}\n")
                    current_charge["active"] = False
                    kwh = round(current_charge['kwh_consumed'], 2)
                    cost = round(kwh * precio_kwh, 2)
                    driver_id = current_charge['driver_id']
                    
                    ticket_message = f"TICKET:{cp_id}:{driver_id}:{kwh}:{cost}:AVERIA"
                    try:
                        if kafka_producer:
                            kafka_producer.send('CPTelemetry', ticket_message)
                            kafka_producer.flush() 
                            print(f"{Colors.MAGENTA}[INFO] Informe final por avería enviado: {kwh} KWh, {cost} €{Colors.RESET}")
                    except Exception as e:
                        print(f"{Colors.RED}[ERROR] No se pudo enviar el informe por avería: {e}{Colors.RESET}")

                    current_charge["driver_id"] = None
                    charge_request_pending = False 

            elif comando.strip().lower() == 'r':
                simular_averia = False
                print(f"{Colors.GREEN}\n[OK] AVERÍA RESUELTA. Engine responderá OK.{Colors.RESET}\n")

            elif comando.strip().lower() == 'e':
                if charge_request_pending:
                    charge_request_pending = False
                    current_charge["active"] = True
                    current_charge["start_time"] = time.time() 
                    print(f"{Colors.GREEN}{Colors.BOLD}\n[>>>] VEHÍCULO ENCHUFADO. Iniciando suministro para {current_charge['driver_id']}...{Colors.RESET}\n")
                else:
                    print(f"{Colors.YELLOW}\n[!] No hay ninguna solicitud de carga pendiente. 'e' no hace nada.{Colors.RESET}\n")

            elif comando.strip().lower() == 'd':
                if current_charge["active"]:
                    print(f"{Colors.MAGENTA}{Colors.BOLD}\n[<<<] VEHÍCULO DESENCHUFADO. Deteniendo suministro para {current_charge['driver_id']}.{Colors.RESET}\n")
                    
                    kwh = round(current_charge['kwh_consumed'], 2)
                    cost = round(kwh * precio_kwh, 2)
                    
                    ticket_message = f"TICKET:{cp_id}:{current_charge['driver_id']}:{kwh}:{cost}"
                    try:
                        if kafka_producer:
                            kafka_producer.send('CPTelemetry', ticket_message)
                            print(f"{Colors.GREEN}[INFO] Ticket final enviado a CENTRAL: {kwh} KWh, {cost} €{Colors.RESET}")
                    except Exception as e:
                        print(f"{Colors.RED}[ERROR] No se pudo enviar el ticket final: {e}{Colors.RESET}")

                    current_charge["active"] = False
                    current_charge["driver_id"] = None
                else:
                    print(f"{Colors.YELLOW}\n[!] No hay ningún vehículo enchufado. 'd' no hace nada.{Colors.RESET}\n")

        except EOFError:
            pass
        except KeyboardInterrupt:
            _original_print("\nSaliendo del bucle de UI...")
            break # Salir del bule while True

def start_kafka_consumer_thread():
    
    # LÓGICA 100% ORIGINAL
    global KAFKA_ADDR, current_charge, cp_id, charge_request_pending
    
    kafka_ip_port_str = f"{KAFKA_ADDR[0]}:{KAFKA_ADDR[1]}"
    
    # (Este print irá al log de la UI)
    print(f"{Colors.YELLOW}[INFO] Hilo consumidor iniciándose...{Colors.RESET}")

    try:
        consumer = KafkaConsumer(
            'commands_to_cp', # <-- ¡¡Corregido el topic ' xd '!!
            bootstrap_servers=kafka_ip_port_str,
            auto_offset_reset='latest',
            api_version=(4, 1, 0)
        )
        print(f"{Colors.GREEN}[INFO] Consumidor de Kafka escuchando en 'commands_to_cp'...{Colors.RESET}")
        for message in consumer:
            msg_str = message.value.decode('utf-8')
            print(f"{Colors.CYAN}[KAFKA-RX] Mensaje (string) recibido: {msg_str}{Colors.RESET}")
            
            parts = msg_str.split(':')
            
            if len(parts) < 2:
                print(f"{Colors.YELLOW}[WARN] Mensaje mal formado, ignorando: {msg_str}{Colors.RESET}")
                continue
                
            command = parts[0]
            target_cp = parts[1]

            if target_cp != cp_id:
                # Silencioso, no es un error
                # print(f"[KAFKA-RX] Mensaje ignorado, no es para mí ({cp_id})")
                continue

            if command == "START":
                if len(parts) == 3:
                    if not current_charge["active"] and not charge_request_pending:
                        driver = parts[2] 
                        
                        current_charge["driver_id"] = driver
                        current_charge["start_time"] = time.time() 
                        current_charge["kwh_consumed"] = 0.0
                        charge_request_pending = True

                        # Esta es la notificación que ves
                        print(f"{Colors.YELLOW}{Colors.BOLD}\n[!!!] SOLICITUD DE CARGA RECIBIDA para {driver} [!!!]{Colors.RESET}")
                        print(f"{Colors.YELLOW}{Colors.BOLD}[..._] Pulse 'e' para ENCHUFAR el vehículo.{Colors.RESET}")
                    else:
                        print(f"{Colors.YELLOW}[WARN] Se recibió START, pero ya estaba cargando.{Colors.RESET}")
                else:
                    print(f"{Colors.YELLOW}[WARN] Mensaje START mal formado, falta driver_id: {msg_str}{Colors.RESET}")
            
            elif command == "STOP":
                if current_charge["active"] or charge_request_pending:
                    print(f"{Colors.MAGENTA}\n[!!!] FIN DE CARGA (orden central) para {current_charge['driver_id']} [!!!]{Colors.RESET}\n")
                    current_charge["active"] = False
                    charge_request_pending = False
                    current_charge["driver_id"] = None
                else:
                    print(f"{Colors.YELLOW}[WARN] Se recibió STOP, pero no estaba cargando.{Colors.RESET}")
            
            else:
                print(f"{Colors.YELLOW}[WARN] Comando desconocido: {command}{Colors.RESET}")
        
    except Exception as e:
        print(f"{Colors.RED}[ERROR-FATAL] El consumidor de Kafka ha fallado: {e}{Colors.RESET}")

def load_config(filename="engine.conf"):
    # LÓGICA 100% ORIGINAL
    config = {}
    config_path = filename
    if not os.path.isabs(config_path):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, filename)

    try:
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                try:
                    clave, valor = line.split('=', 1)
                    clave = clave.strip()
                    valor = valor.strip()
                    if 'PUERTO' in clave:
                        config[clave] = int(valor)
                    else:
                        config[clave] = valor
                except ValueError:
                    _original_print(f"{Colors.YELLOW}[WARN] Línea mal formada en {filename}: {line}{Colors.RESET}")
        
        _original_print(f"{Colors.GREEN}[INFO] Configuración cargada desde {filename}{Colors.RESET}")
        return config

    except FileNotFoundError:
        _original_print(f"{Colors.RED}[ERROR-FATAL] No se encontró el archivo de configuración: {filename}{Colors.RESET}")
        return None
    except Exception as e:
        _original_print(f"{Colors.RED}[ERROR-FATAL] No se pudo leer el archivo de configuración: {e}{Colors.RESET}")
        return None

def start_monitor_server():
    # LÓGICA 100% ORIGINAL
    global ENGINE_LISTEN_ADDR
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(ENGINE_LISTEN_ADDR) 
            s.listen()
            _original_print(f"{Colors.GREEN}[INFO] Servidor del Monitor escuchando en {ENGINE_LISTEN_ADDR}...{Colors.RESET}")
            _add_log(f"{Colors.GREEN}Monitor Server OK{Colors.RESET}") # Log para la UI

            while True:
                conn, addr = s.accept()
                client_thread = threading.Thread(
                    target=handle_monitor_connection, 
                    args=(conn, addr),
                    daemon=True
                )
                client_thread.start()
                
    except Exception as e:
        _original_print(f"{Colors.RED}[ERROR-FATAL] El servidor del Monitor ha fallado: {e}{Colors.RESET}")
        _add_log(f"{Colors.RED}Monitor Server CAÍDO: {e}{Colors.RESET}") # Log para la UI

def handle_monitor_connection(conn, addr):
    # LÓGICA 100% ORIGINAL
    global simular_averia, UI_MONITOR_CLIENT
    
    print(f"{Colors.CYAN}[OK] Monitor conectado desde {addr}{Colors.RESET}")
    with UI_LOCK:
        UI_MONITOR_CLIENT = str(addr) # Actualizar UI
    
    try:
        while True:
            mensaje = conn.recv(1024).decode('utf-8')
            if not mensaje:
                print(f"{Colors.CYAN}[INFO] Monitor {addr} desconectado.{Colors.RESET}")
                break
            
            if "HEALTH_CHECK" in mensaje:
                if simular_averia:
                    conn.sendall(b"KO: Falla simulada")
                else:
                    conn.sendall(b"OK")
            else:
                print(f"{Colors.YELLOW}[WARN] Mensaje inesperado del Monitor: {mensaje}{Colors.RESET}")
                
    except Exception as e:
        print(f"{Colors.YELLOW}[ERROR] Error con el Monitor {addr}: {e}{Colors.RESET}")
    finally:
        with UI_LOCK:
            UI_MONITOR_CLIENT = "N/A" # Actualizar UI
        conn.close()

 ###################################
#
# MAIN
#       
 ###################################

def __main__():
    global KAFKA_ADDR, ENGINE_LISTEN_ADDR, cp_id, precio_kwh, kafka_producer
    global UI_KAFKA_ADDR, UI_MONITOR_ADDR, UI_CP_ID # Globales de UI

    # 1. Leer argumentos de Kafka
    if len(sys.argv) != 3:
        _original_print(f"{Colors.RED}Uso: python EV_CP_E.py <IP_KAFKA> <PUERTO_KAFKA>{Colors.RESET}")
        return
    
    KAFKA_ADDR = (sys.argv[1], int(sys.argv[2]))
    kafka_ip_port_str = f"{KAFKA_ADDR[0]}:{KAFKA_ADDR[1]}"

    # 2. Cargar la configuración desde el archivo
    config = load_config("engine.conf")
    if config is None:
        return 

    # 3. Asignar las variables globales desde la configuración
    try:
        ENGINE_LISTEN_ADDR = (config['IP_ESCUCHA'], config['PUERTO_ESCUCHA'])
        cp_id = config['CP_ID']
        precio_kwh = float(config['PRECIO_KWH'])
        
        # Asignar globales de UI (NUEVO)
        UI_CP_ID = cp_id
        UI_KAFKA_ADDR = f"{KAFKA_ADDR[0]}:{KAFKA_ADDR[1]}"
        UI_MONITOR_ADDR = f"{ENGINE_LISTEN_ADDR[0]}:{ENGINE_LISTEN_ADDR[1]}"

    except KeyError as e:
        _original_print(f"{Colors.RED}[ERROR-FATAL] Falta una clave en el archivo de configuración: {e}{Colors.RESET}")
        return
    except ValueError:
        _original_print(f"{Colors.RED}[ERROR-FATAL] El PRECIO_KWH en engine.conf no es un número válido.{Colors.RESET}")
        return

    _original_print(f"{Colors.CYAN}[INFO] Engine iniciado para CP_ID: {cp_id} (Precio: {precio_kwh} €/KWh){Colors.RESET}")
    _original_print(f"{Colors.CYAN}[INFO] Conectará a Kafka en {KAFKA_ADDR}{Colors.RESET}")
    
    # 4. Inicializar el productor de Kafka
    create_kafka_producer(kafka_ip_port_str)
    if kafka_producer is None:
        _original_print(f"{Colors.RED}[ERROR-FATAL] Saliendo porque el productor de Kafka no pudo iniciarse.{Colors.RESET}")
        return 

    # --- Creación de todos los Hilos (LÓGICA ORIGINAL) ---
    
    # Hilo 1: Escucha al Monitor (Socket)
    monitor_server_thread = threading.Thread(
        target=start_monitor_server, 
        daemon=True
    )
    
    # Hilo 2: Interfaz de Usuario
    ui_thread = threading.Thread(
        target=user_interface_thread, 
        daemon=False # Este es el hilo principal
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

    # --- Lanzar todos los hilos (LÓGICA ORIGINAL) ---
    monitor_server_thread.start()
    kafka_consumer_thread.start()
    supply_sim_thread.start()
    ui_thread.start() # Iniciar la UI al final

    # --- Espera y Limpieza (LÓGICA ORIGINAL) ---
    try:
        ui_thread.join() # El script espera a que el hilo de UI termine (Ctrl+C)
    except KeyboardInterrupt:
        pass 
    
    # Restaurar print original al salir
    print = _original_print
    
    if kafka_producer:
        kafka_producer.close()
    print(f"\n{Colors.YELLOW}[INFO] Cerrando Engine...{Colors.RESET}")

if __name__ == "__main__":
    __main__()