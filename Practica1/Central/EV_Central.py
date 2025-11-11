# -*- coding: utf-8 -*-
from kafka import KafkaConsumer, KafkaProducer
import socket
import threading
import sys
import os
import time
from typing import List, Dict, Any, Optional, Tuple

# --- 0. VISUALS ---
if os.name == 'nt':
    os.system('color') 

def _clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BG_GREEN = '\033[42m'
    BG_ORANGE = '\033[48;5;208m'
    BG_RED = '\033[41m'
    BG_GREY = '\033[100m'

# --- 1. CONFIGURATION AND CONSTANTS ---
KAFKA_REQUEST_TOPIC = 'DriverRequest'
KAFKA_RESPONSE_TOPIC = 'DriverResponse'
KAFKA_TELEMETRY_TOPIC = 'CPTelemetry'
KAFKA_ENGINE_TOPIC = 'commands_to_cp'
KAFKA_BROKER_ADDR = None

FAST_INIT_TIMEOUT = 10001
CONSUMER_SESSION_TIMEOUT = 10000
CONSUMER_HEARTBEAT = 3000

HOST = '0.0.0.0' # Escucha en todas las interfaces
FORMAT = 'utf-8'

# --- Status Definitions ---
STATUS_ACTIVO = 'ACTIVO'
STATUS_SUMINISTRANDO = 'SUMINISTRANDO'
STATUS_PARADO = 'PARADO'
STATUS_AVERIA = 'AVERIADO'
STATUS_DESCONECTADO = 'DESCONECTADO'

# --- 2. DATA STRUCTURES ---
# Registros en memoria del estado de CPs y drivers
cp_registry: dict[str, dict] = {}
driver_registry: set[str] = set()

# --- GLOBALES UI ---
UI_LOCK = threading.Lock() # Protege el acceso concurrente a logs y telemetría
UI_LAST_LOGS = []
cp_telemetry: Dict[str, Dict[str, Any]] = {}
ongoing_requests: List[Dict[str, str]] = []

# Productor de Kafka global y persistente
KAFKA_PRODUCER: Optional[KafkaProducer] = None
PRODUCER_LOCK = threading.Lock()

# --- 3. UTILITY FUNCTIONS ---

def _add_log(message: str, color: str = Colors.WHITE):
    """Añade un mensaje al log de la UI (thread-safe)."""
    with UI_LOCK:
        timestamp = time.strftime('%H:%M:%S')
        log_entry = f"{Colors.CYAN}[{timestamp}]{Colors.RESET} {color}{message}{Colors.RESET}"
        UI_LAST_LOGS.append(log_entry)
        if len(UI_LAST_LOGS) > 8:
            UI_LAST_LOGS.pop(0)

def build_protocol_response(message_type: str, payload: str = "") -> bytes:
    """Construye un mensaje de respuesta para el protocolo socket."""
    return f"{message_type}:{payload}".encode(FORMAT)

def parse_protocol(data: bytes) -> Optional[str]:
    """Decodifica y limpia un mensaje de protocolo socket."""
    try:
        return data.decode(FORMAT).strip()
    except UnicodeDecodeError:
        _add_log("ERROR: Mensaje socket corrupto (decode).", Colors.RED)
        return None

def reset_cp_state():
    """Resetea el estado de todos los CPs a DESCONECTADO al arrancar."""
    input_file = "ChargingPoints.txt"
    updated_lines = []
    try:
        with open(input_file, "r") as file:
            for line in file:
                line = line.strip()
                if not line: continue
                parts = line.split(':')
                if len(parts) >= 4:
                    cp_id, location, price = parts[0], parts[1], parts[2]
                    updated_lines.append(f"{cp_id}:{location}:{price}:{STATUS_DESCONECTADO}\n")
        with open(input_file, "w") as file:
            file.writelines(updated_lines)
        print(f"[INIT] Estados reseteados a {STATUS_DESCONECTADO} en '{input_file}'.")
    except FileNotFoundError:
        print(f"[WARNING] No se encontro '{input_file}'. Se creará uno nuevo.")
    except Exception as e:
        print(f"[ERROR] Error reseteando fichero: {e}")

def read_data_cp():
    """Carga los CPs conocidos desde ChargingPoints.txt a memoria."""
    try:
        file = open("ChargingPoints.txt", "r")
        for line in file:
            parts = line.strip().split(':')
            if len(parts) >= 4:
                cp_id, location, price, status = parts[0], parts[1], parts[2], parts[3]
                cp_registry[cp_id] = {
                    "location": location,
                    "price": float(price),
                    "status": status, 
                    "addr": None
                }
        file.close()
        print(f"[INIT] {len(cp_registry)} CPs cargados.")
    except FileNotFoundError:
        print("[WARNING] ChargingPoints.txt no encontrado.")

def write_data_cp():
    """Persiste el estado actual de cp_registry en ChargingPoints.txt."""
    try:
        file = open("ChargingPoints.txt", "w")
        for cp_id, info in cp_registry.items():
            line = f"{cp_id}:{info['location']}:{info['price']}:{info['status']}\n"
            file.write(line)
        file.close()
    except Exception as e:
        _add_log(f"ERROR escribiendo ChargingPoints.txt: {e}", Colors.RED)

def read_data_driver():
    """Carga los drivers conocidos desde Drivers.txt a memoria."""
    global driver_registry
    try:
        with open("Drivers.txt", "r") as file:
            for line in file:
                if line.strip(): driver_registry.add(line.strip())
        print(f"[INIT] {len(driver_registry)} Drivers cargados.")
    except FileNotFoundError:
        print("[WARNING] Drivers.txt no encontrado.")

def write_data_driver():
    """Persiste el set de drivers en Drivers.txt."""
    global driver_registry
    try:
        with open("Drivers.txt", "w") as file:
            for driver_id in driver_registry:
                file.write(f"{driver_id}\n")
    except Exception as e:
        _add_log(f"ERROR escribiendo Drivers.txt: {e}", Colors.RED)

# --- 4. KAFKA PRODUCER/CONSUMER ---

def _initialize_producer() -> bool:
    """Inicializa el productor global de Kafka."""
    global KAFKA_PRODUCER, KAFKA_BROKER_ADDR
    if KAFKA_PRODUCER is not None:
        return True
    
    _add_log(f"Intentando conectar productor Kafka a {KAFKA_BROKER_ADDR}...", Colors.YELLOW)
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_ADDR,
            api_version=(4, 1, 0),
            request_timeout_ms=FAST_INIT_TIMEOUT,
            value_serializer=lambda v: v.encode('utf-8')
        )
        KAFKA_PRODUCER = producer
        _add_log("Productor Kafka global CONECTADO.", Colors.GREEN)
        return True
    except Exception as e:
        _add_log(f"Fallo al crear productor Kafka global: {e}", Colors.RED)
        return False

def send_kafka_message(topic: str, message: str) -> bool:
    """Envía un mensaje Kafka usando el productor global (thread-safe)."""
    global KAFKA_PRODUCER
    with PRODUCER_LOCK:
        if KAFKA_PRODUCER is None:
            _add_log("Productor Kafka nulo, intentando reconectar...", Colors.YELLOW)
            if not _initialize_producer():
                _add_log(f"Fallo de envío (Kafka): {topic} - {message}", Colors.RED)
                return False
        
        try:
            _add_log(f"Kafka ENVIANDO a {topic}: {message}", Colors.CYAN) 
            future = KAFKA_PRODUCER.send(topic, value=message)
            future.get(timeout=10) 
            return True
        except Exception as e:
            _add_log(f"Error al enviar Kafka ({topic}): {e}", Colors.RED)
            # Si falla, se resetea el productor para el próximo reintento
            if KAFKA_PRODUCER:
                KAFKA_PRODUCER.close()
            KAFKA_PRODUCER = None
            return False

def authenticate_driver(driver_id: str):
    """Autentica o registra un nuevo driver si no existe."""
    if driver_id not in driver_registry:
        _add_log(f"AUTH: Nuevo driver '{driver_id}' registrado.", Colors.MAGENTA)
        driver_registry.add(driver_id)
        write_data_driver()
    return True

def _check_and_authorize_cp(driver_id_received: str, cp_id_received: str):
    """Valida una solicitud de carga (REQUEST) de un driver."""
    with UI_LOCK:
        ongoing_requests.append({
            'date': time.strftime('%d/%m/%y'), 'time': time.strftime('%H:%M'),
            'user_id': driver_id_received, 'cp_id': cp_id_received
        })
        if len(ongoing_requests) > 5: ongoing_requests.pop(0)

    if not authenticate_driver(driver_id_received): return

    if cp_id_received in cp_registry:
        cp_info = cp_registry[cp_id_received]
        if cp_info['status'] == STATUS_ACTIVO:
            # Poner el CP en SUMINISTRANDO para bloquearlo
            cp_registry[cp_id_received]['status'] = STATUS_SUMINISTRANDO
            write_data_cp()
            with UI_LOCK:
                cp_telemetry[cp_id_received] = {'kwh': 0.0, 'cost': 0.0, 'driver': driver_id_received}

            # Enviar orden START al Engine y ACEPTADO al Driver
            if send_kafka_message(KAFKA_ENGINE_TOPIC, f"START:{cp_id_received}:{driver_id_received}"):
                _add_log(f"AUTORIZADO: {driver_id_received} -> {cp_id_received}", Colors.GREEN)
                send_kafka_message(KAFKA_RESPONSE_TOPIC, f"ACEPTADO:{driver_id_received}:{cp_id_received}")
            else:
                # Rollback si falla el envío al Engine
                cp_registry[cp_id_received]['status'] = STATUS_ACTIVO
                _add_log(f"ERROR KAFKA: Fallo START a {cp_id_received}", Colors.RED)
                send_kafka_message(KAFKA_RESPONSE_TOPIC, f"RECHAZADO:{driver_id_received}:{cp_id_received}")
        else:
            # CP no está ACTIVO (AVERIADO, PARADO, etc.)
            _add_log(f"RECHAZADO: CP {cp_id_received} no ACTIVO ({cp_info['status']})", Colors.YELLOW)
            send_kafka_message(KAFKA_RESPONSE_TOPIC, f"RECHAZADO:{driver_id_received}:{cp_id_received}")
    else:
        # CP_ID desconocido
        _add_log(f"RECHAZADO: CP {cp_id_received} desconocido", Colors.YELLOW)
        send_kafka_message(KAFKA_RESPONSE_TOPIC, f"RECHAZADO:{driver_id_received}:{cp_id_received}")

def read_consumer():
    """Hilo consumidor principal de Kafka (topics Request y Telemetry)."""
    global KAFKA_BROKER_ADDR
    topics = [KAFKA_REQUEST_TOPIC, KAFKA_TELEMETRY_TOPIC]
    
    while True: # Bucle de reconexión
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BROKER_ADDR,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='ev-central-group-MAIN',
                api_version=(4, 1, 0),
                request_timeout_ms=FAST_INIT_TIMEOUT,
                session_timeout_ms=CONSUMER_SESSION_TIMEOUT,
                heartbeat_interval_ms=CONSUMER_HEARTBEAT
            )
            _add_log("Kafka Consumer conectado.", Colors.GREEN)
            
            for message in consumer:
                msg_str = message.value.decode('utf-8')
                parts = msg_str.split(':')
                
                if message.topic == KAFKA_REQUEST_TOPIC:
                    _add_log(f"Kafka RECIBIDO (Request): {msg_str}", Colors.BLUE) 
                    
                    if msg_str.startswith("REQUEST:") and len(parts) >= 3:
                        _check_and_authorize_cp(parts[1], parts[2])
                    
                    elif msg_str.startswith("CP_REQUEST:") and len(parts) >= 2:
                        # Driver pide la lista de CPs activos
                        active_cps = [f"{k}({v['location']})@{v['price']}€" 
                                      for k, v in cp_registry.items() if v['status'] == STATUS_ACTIVO]
                        payload = ":".join(active_cps) if active_cps else ""
                        send_kafka_message(KAFKA_RESPONSE_TOPIC, f"CP_LIST:{payload}")
                
                elif message.topic == KAFKA_TELEMETRY_TOPIC:
                    if msg_str.startswith("SUMINISTRANDO:") and len(parts) >= 4:
                        # Telemetría en tiempo real del Engine
                        cp_id, kwh, cost = parts[1], float(parts[2]), float(parts[3])
                        if cp_id in cp_registry and cp_registry[cp_id]['status'] != STATUS_PARADO:
                             cp_registry[cp_id]['status'] = STATUS_SUMINISTRANDO
                        with UI_LOCK:
                            if cp_id in cp_telemetry:
                                cp_telemetry[cp_id]['kwh'] = kwh
                                cp_telemetry[cp_id]['cost'] = cost

                    elif msg_str.startswith("TICKET:") and len(parts) >= 5:
                        # Engine notifica fin de carga (Ticket)
                        cp_id, driver_id = parts[1], parts[2]
                        send_kafka_message(KAFKA_RESPONSE_TOPIC, msg_str) # Reenviar ticket al Driver
                        
                        if len(parts) == 6 and parts[5] == "AVERIA":
                             _add_log(f"TICKET AVERÍA de CP {cp_id}", Colors.RED)
                             cp_registry[cp_id]['status'] = STATUS_AVERIA
                        else:
                            _add_log(f"TICKET FINAL de CP {cp_id}", Colors.BLUE)
                            # Si no estaba parado por admin, volver a ACTIVO
                            if cp_registry[cp_id]['status'] != STATUS_PARADO:
                                 cp_registry[cp_id]['status'] = STATUS_ACTIVO
                        
                        write_data_cp()
                        with UI_LOCK:
                            if cp_id in cp_telemetry: del cp_telemetry[cp_id]
                            global ongoing_requests
                            ongoing_requests = [r for r in ongoing_requests if not (r['user_id'] == driver_id and r['cp_id'] == cp_id)]

        except Exception as e:
            _add_log(f"FATAL KAFKA CONSUMER: {e}. Reintentando en 5s...", Colors.BG_RED)
            time.sleep(5) 

# --- 5. SOCKET SERVER ---

def handle_client(conn, addr):
    """Maneja una conexión socket (Monitor) en un hilo dedicado."""
    current_cp_id = None
    try:
        while True:
            data = conn.recv(1024)
            if not data: break # Conexión cerrada por el Monitor
            
            msg = parse_protocol(data)
            if not msg: continue
            
            parts = msg.split(':')
            msg_type = parts[0].upper()

            if msg_type == "PING": # Heartbeat del Monitor
                current_cp_id = parts[1]
                continue

            elif msg_type == "REGISTRO":
                cp_id, loc, price = parts[1], parts[2], float(parts[3])
                current_cp_id = cp_id
                cp_registry[cp_id] = {"location": loc, "price": price, "status": STATUS_ACTIVO, "addr": addr}
                write_data_cp()
                conn.sendall(build_protocol_response("ACEPTADO", cp_id))
                _add_log(f"REGISTRO: CP {cp_id}", Colors.GREEN)

            elif msg_type == "ESTADO":
                # Monitor reporta un cambio de estado (p.ej. avería del Engine)
                cp_id, new_status = parts[1], parts[2].upper()
                if cp_id in cp_registry:
                    if new_status == STATUS_AVERIA and cp_registry[cp_id]["status"] == STATUS_SUMINISTRANDO:
                         _add_log(f"¡ALERTA! Avería en CP {cp_id} durante suministro.", Colors.BG_RED)
                    cp_registry[cp_id]["status"] = new_status
                    write_data_cp()
                    conn.sendall(build_protocol_response("ACTUALIZADO", cp_id))
                    _add_log(f"ESTADO CP {cp_id} -> {new_status}", Colors.YELLOW if new_status != STATUS_AVERIA else Colors.RED)

            elif msg_type == "AUTENTICACION":
                cp_id = parts[1]
                if cp_id in cp_registry:
                    current_cp_id = cp_id
                    
                    # Si se reconecta tras avería o desconexión, se reactiva
                    current_status = cp_registry[cp_id]["status"]
                    if current_status == STATUS_DESCONECTADO or current_status == STATUS_AVERIA:
                         cp_registry[cp_id]["status"] = STATUS_ACTIVO
                         write_data_cp() 
                         
                    conn.sendall(build_protocol_response("ACEPTADO", cp_id))
                    _add_log(f"RECONEXIÓN: CP {cp_id}", Colors.CYAN)
                else:
                    # CP desconocido, se rechaza para que Monitor inicie registro
                    conn.sendall(build_protocol_response("RECHAZADO", "")) 

    except Exception: 
        pass # Captura genérica para desconexiones abruptas
    finally:
        # Si el Monitor se desconecta (Ctrl+C o error), se marca como AVERIADO
        if current_cp_id and current_cp_id in cp_registry:
             cp_registry[current_cp_id]["status"] = STATUS_AVERIA
             write_data_cp()
             _add_log(f"DESCONEXIÓN MONITOR (AVERÍA): CP {current_cp_id}", Colors.RED)
        conn.close()

def socket_server_thread(port):
    """Hilo principal que acepta conexiones socket de Monitores."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.bind((HOST, port))
        server.listen()
        _add_log(f"Socket Server en puerto {port}", Colors.GREEN)
        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except Exception as e:
         _add_log(f"FATAL SOCKET SERVER: {e}", Colors.BG_RED)

# --- 6. UI RENDERER ---

def ui_renderer_thread():
    """Refresca el panel de monitorización (UI) cada segundo."""
    while True:
        time.sleep(1)
        _clear_screen()
        print(f"{Colors.BLUE}{Colors.BOLD}*** SD EV CHARGING SOLUTION. MONITORIZATION PANEL ***{Colors.RESET}")
        
        cp_ids = sorted(cp_registry.keys())
        for i in range(0, len(cp_ids), 4):
            row_ids = cp_ids[i:i+4]
            # Líneas de la cuadrícula
            line1, line2, line3, line4 = "", "", "", ""
            
            for cp_id in row_ids:
                if cp_id not in cp_registry: continue # Seguridad por si se borra en concurrencia
                
                info = cp_registry[cp_id]
                st = info['status']
                
                # Determinar color de fondo
                bg = Colors.BG_GREY
                if st in [STATUS_ACTIVO, STATUS_SUMINISTRANDO]: bg = Colors.BG_GREEN
                elif st == STATUS_PARADO: bg = Colors.BG_ORANGE
                elif st == STATUS_AVERIA: bg = Colors.BG_RED
                
                # Construir líneas
                line1 += f"{bg}{Colors.WHITE} {cp_id:^18} {Colors.RESET} "
                line2 += f"{bg}{Colors.WHITE} {info['location'][:18]:^18} {Colors.RESET} "
                
                txt3, txt4 = f"{info['price']}€/kWh", ""
                if st == STATUS_SUMINISTRANDO:
                    tele = cp_telemetry.get(cp_id, {})
                    txt3 = f"Driver {tele.get('driver', '?')}"
                    txt4 = f"{tele.get('kwh', 0.0):.1f}kWh | {tele.get('cost', 0.0):.2f}€"
                elif st == STATUS_PARADO:
                    txt3 = "Out of Order"
                elif st == STATUS_AVERIA:
                    txt3 = "AVERIADO"
                elif st == STATUS_DESCONECTADO:
                    txt3 = "DESCONECTADO"

                line3 += f"{bg}{Colors.WHITE} {txt3:^18} {Colors.RESET} "
                line4 += f"{bg}{Colors.WHITE} {txt4:^18} {Colors.RESET} "

            print(line1 + "\n" + line2 + "\n" + line3 + "\n" + line4 + "\n")

        # Mostrar logs y solicitudes
        print(f"{Colors.CYAN}{Colors.BOLD}*** ON_GOING DRIVERS REQUESTS ***{Colors.RESET}")
        print(f"{Colors.BOLD}DATE      START TIME   User ID    CP{Colors.RESET}")
        with UI_LOCK:
            for r in ongoing_requests: print(f"{r['date']:<10} {r['time']:<12} {r['user_id']:<10} {r['cp_id']}")
        print(f"\n{Colors.CYAN}{Colors.BOLD}*** APPLICATION MESSAGES ***{Colors.RESET}")
        with UI_LOCK:
            for log in UI_LAST_LOGS: print(log)
        
        print(f"{Colors.CYAN}--------------------------------------------{Colors.RESET}")
        print("Admin: [p ID] Parar CP | [r ID] Reanudar CP | [q] Salir")

# --- 7. ADMIN INPUT LOOP ---
def admin_input_loop():
    """Maneja la entrada de administrador (stdin) para parar/reanudar CPs."""
    while True:
        try:
            cmd = sys.stdin.readline().strip()
            if not cmd: continue
            parts = cmd.lower().split(' ')
            
            if parts[0] == 'q':
                _add_log("Cerrando...", Colors.MAGENTA)
                if KAFKA_PRODUCER: KAFKA_PRODUCER.close() 
                os._exit(0)

            elif parts[0] == 'p' and len(parts) >= 2: # PAUSE
                cp_id = parts[1].upper()
                if cp_id in cp_registry:
                    current_status = cp_registry[cp_id]['status']
                    
                    # No se puede pausar un CP que no esté operativo
                    if current_status == STATUS_DESCONECTADO:
                         _add_log(f"ERROR: CP {cp_id} está DESCONECTADO. No se puede parar.", Colors.RED)
                         continue
                    if current_status == STATUS_AVERIA:
                         _add_log(f"ERROR: CP {cp_id} ya está AVERIADO. No se puede pausar.", Colors.RED)
                         continue
                         
                    cp_registry[cp_id]['status'] = STATUS_PARADO
                    write_data_cp()
                    send_kafka_message(KAFKA_ENGINE_TOPIC, f"PAUSE:{cp_id}")
                    _add_log(f"ADMIN: CP {cp_id} PAUSADO (Out of Order)", Colors.YELLOW)
                else:
                     _add_log(f"ADMIN ERROR: CP {cp_id} no encontrado", Colors.RED)

            elif parts[0] == 'r' and len(parts) >= 2: # RESUME
                cp_id = parts[1].upper()
                if cp_id in cp_registry:
                    # Solo se puede reanudar si estaba PARADO
                    if cp_registry[cp_id]['status'] == STATUS_PARADO:
                        cp_registry[cp_id]['status'] = STATUS_ACTIVO
                        write_data_cp()
                        send_kafka_message(KAFKA_ENGINE_TOPIC, f"RESUME:{cp_id}")
                        _add_log(f"ADMIN: CP {cp_id} REANUDADO a ACTIVO", Colors.GREEN)
                    else:
                        _add_log(f"ADMIN: CP {cp_id} no estaba PARADO", Colors.YELLOW)

        except Exception: pass

# --- MAIN ---
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Uso: python EV_Central.py <PUERTO_SOCKET> <IP_KAFKA> <PUERTO_KAFKA>")
        sys.exit(1)

    SOCKET_PORT = int(sys.argv[1])
    KAFKA_BROKER_ADDR = [f'{sys.argv[2]}:{sys.argv[3]}'] 

    # Inicializar productor global antes de nada
    if not _initialize_producer():
        print("Fallo crítico al inicializar el productor de Kafka. Saliendo.")
        sys.exit(1)

    reset_cp_state()
    read_data_cp()
    read_data_driver()

    # Lanzar hilos de trabajo
    threading.Thread(target=read_consumer, daemon=True).start()
    threading.Thread(target=socket_server_thread, args=(SOCKET_PORT,), daemon=True).start()
    threading.Thread(target=ui_renderer_thread, daemon=True).start()

    try:
        admin_input_loop()
    except KeyboardInterrupt:
        if KAFKA_PRODUCER: KAFKA_PRODUCER.close() 
        write_data_cp()
        sys.exit(0)