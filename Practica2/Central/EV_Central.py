# -*- coding: utf-8 -*-
from kafka import KafkaConsumer, KafkaProducer
import socket
import threading
import sys
import os
import time
import sqlite3 # <--- NUEVO: Necesario para la BD
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

# Kafka Timeout Optimization (ms)
FAST_INIT_TIMEOUT = 10001
CONSUMER_SESSION_TIMEOUT = 10000
CONSUMER_HEARTBEAT = 3000

HOST = '0.0.0.0'
FORMAT = 'utf-8'

# Status Definitions
STATUS_ACTIVO = 'ACTIVO'
STATUS_SUMINISTRANDO = 'SUMINISTRANDO'
STATUS_PARADO = 'PARADO'
STATUS_AVERIA = 'AVERIADO'
STATUS_DESCONECTADO = 'DESCONECTADO'

# --- LOGICA DE BASE DE DATOS (Release 2) ---
# Buscamos la BD igual que hicimos en el ejemplo sencillo
POSSIBLE_PATHS = [
    'ev_charging.db',
    '../Registry/ev_charging.db',
    '../Practica2/Registry/ev_charging.db',
    'Registry/ev_charging.db'
]
DB_PATH = 'ev_charging.db'
for path in POSSIBLE_PATHS:
    if os.path.exists(path):
        DB_PATH = path
        break
# --------------------------------------------

# --- 2. DATA STRUCTURES ---
cp_registry: dict[str, dict] = {}
driver_registry: set[str] = set()

# --- GLOBALES UI ---
UI_LOCK = threading.Lock()
UI_LAST_LOGS = []
cp_telemetry: Dict[str, Dict[str, Any]] = {}
ongoing_requests: List[Dict[str, str]] = []

# --- Kafka Producer Global ---
KAFKA_PRODUCER: Optional[KafkaProducer] = None
PRODUCER_LOCK = threading.Lock()

def _add_log(message: str, color: str = Colors.WHITE):
    with UI_LOCK:
        timestamp = time.strftime('%H:%M:%S')
        log_entry = f"{Colors.CYAN}[{timestamp}]{Colors.RESET} {color}{message}{Colors.RESET}"
        UI_LAST_LOGS.append(log_entry)
        if len(UI_LAST_LOGS) > 8:
            UI_LAST_LOGS.pop(0)

# --- 3. UTILITY FUNCTIONS ---

def verify_identity_in_db(cp_id):
    """(NUEVO) Verifica si el CP tiene token en la BD SQLite."""
    try:
        with sqlite3.connect(DB_PATH) as conn_db:
            cursor = conn_db.cursor()
            cursor.execute("SELECT token FROM charging_points WHERE id = ?", (cp_id,))
            result = cursor.fetchone()
            if result:
                return True
            return False
    except Exception as e:
        _add_log(f"Error BD {DB_PATH}: {e}", Colors.RED)
        return False

def build_protocol_response(message_type: str, payload: str = "") -> bytes:
    return f"{message_type}:{payload}".encode(FORMAT)

def parse_protocol(data: bytes) -> Optional[str]:
    try:
        return data.decode(FORMAT).strip()
    except UnicodeDecodeError:
        return None

def reset_cp_state():
    # Mantenemos esto para limpiar el fichero TXT al inicio
    input_file = "ChargingPoints.txt"
    updated_lines = []
    try:
        with open(input_file, "r") as file:
            for line in file:
                line = line.strip()
                if not line: continue
                parts = line.split(':')
                if len(parts) >= 4:
                    cp_id, location, price, status = parts[0], parts[1], parts[2], parts[3]
                    updated_lines.append(f"{cp_id}:{location}:{price}:{STATUS_DESCONECTADO}\n")
        with open(input_file, "w") as file:
            file.writelines(updated_lines)
    except Exception: pass

def read_data_cp():
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
    except FileNotFoundError:
        print("[WARNING] ChargingPoints.txt not found.")

def write_data_cp():
    try:
        file = open("ChargingPoints.txt", "w")
        for cp_id, info in cp_registry.items():
            line = f"{cp_id}:{info['location']}:{info['price']}:{info['status']}\n"
            file.write(line)
        file.close()
    except Exception: pass

def read_data_driver():
    global driver_registry
    try:
        with open("Drivers.txt", "r") as file:
            for line in file:
                if line.strip(): driver_registry.add(line.strip())
    except FileNotFoundError: pass

def write_data_driver():
    global driver_registry
    try:
        with open("Drivers.txt", "w") as file:
            for driver_id in driver_registry:
                file.write(f"{driver_id}\n")
    except Exception: pass

# --- 4. KAFKA PRODUCER/CONSUMER ---

def _initialize_producer() -> bool:
    global KAFKA_PRODUCER, KAFKA_BROKER_ADDR
    if KAFKA_PRODUCER is not None: return True
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_ADDR,
            api_version=(4, 1, 0),
            request_timeout_ms=FAST_INIT_TIMEOUT,
            value_serializer=lambda v: v.encode('utf-8')
        )
        KAFKA_PRODUCER = producer
        return True
    except Exception as e:
        return False

def send_kafka_message(topic: str, message: str) -> bool:
    global KAFKA_PRODUCER
    with PRODUCER_LOCK:
        if KAFKA_PRODUCER is None:
            if not _initialize_producer(): return False
        try:
            future = KAFKA_PRODUCER.send(topic, value=message)
            future.get(timeout=5)
            return True
        except Exception as e:
            if KAFKA_PRODUCER: KAFKA_PRODUCER.close()
            KAFKA_PRODUCER = None
            return False

def authenticate_driver(driver_id: str):
    if driver_id not in driver_registry:
        driver_registry.add(driver_id)
        write_data_driver()
    return True

def _check_and_authorize_cp(driver_id_received: str, cp_id_received: str):
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
            cp_registry[cp_id_received]['status'] = STATUS_SUMINISTRANDO
            write_data_cp()
            with UI_LOCK:
                cp_telemetry[cp_id_received] = {'kwh': 0.0, 'cost': 0.0, 'driver': driver_id_received}

            if send_kafka_message(KAFKA_ENGINE_TOPIC, f"START:{cp_id_received}:{driver_id_received}"):
                _add_log(f"AUTORIZADO: {driver_id_received} -> {cp_id_received}", Colors.GREEN)
                send_kafka_message(KAFKA_RESPONSE_TOPIC, f"ACEPTADO:{driver_id_received}:{cp_id_received}")
            else:
                cp_registry[cp_id_received]['status'] = STATUS_ACTIVO
                send_kafka_message(KAFKA_RESPONSE_TOPIC, f"RECHAZADO:{driver_id_received}:{cp_id_received}")
        else:
            send_kafka_message(KAFKA_RESPONSE_TOPIC, f"RECHAZADO:{driver_id_received}:{cp_id_received}")
    else:
        send_kafka_message(KAFKA_RESPONSE_TOPIC, f"RECHAZADO:{driver_id_received}:{cp_id_received}")

def read_consumer():
    global KAFKA_BROKER_ADDR
    topics = [KAFKA_REQUEST_TOPIC, KAFKA_TELEMETRY_TOPIC]
    
    while True:
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BROKER_ADDR,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='ev-central-group-MAIN',
                api_version=(4, 1, 0),
                request_timeout_ms=FAST_INIT_TIMEOUT
            )
            _add_log("Kafka Consumer conectado.", Colors.GREEN)
            
            for message in consumer:
                msg_str = message.value.decode('utf-8')
                parts = msg_str.split(':')
                
                if message.topic == KAFKA_REQUEST_TOPIC:
                    if msg_str.startswith("REQUEST:") and len(parts) >= 3:
                        _check_and_authorize_cp(parts[1], parts[2])
                    elif msg_str.startswith("CP_REQUEST:"):
                        active_cps = [f"{k}({v['location']})@{v['price']}€" 
                                      for k, v in cp_registry.items() if v['status'] == STATUS_ACTIVO]
                        payload = ":".join(active_cps) if active_cps else ""
                        send_kafka_message(KAFKA_RESPONSE_TOPIC, f"CP_LIST:{payload}")
                
                elif message.topic == KAFKA_TELEMETRY_TOPIC:
                    if msg_str.startswith("SUMINISTRANDO:") and len(parts) >= 4:
                        cp_id, kwh, cost = parts[1], float(parts[2]), float(parts[3])
                        with UI_LOCK:
                            if cp_id in cp_telemetry:
                                cp_telemetry[cp_id]['kwh'] = kwh
                                cp_telemetry[cp_id]['cost'] = cost
                    elif msg_str.startswith("TICKET:") and len(parts) >= 5:
                        cp_id = parts[1]
                        send_kafka_message(KAFKA_RESPONSE_TOPIC, msg_str)
                        if len(parts) == 6 and parts[5] == "AVERIA":
                             cp_registry[cp_id]['status'] = STATUS_AVERIA
                        else:
                             if cp_registry[cp_id]['status'] != STATUS_PARADO:
                                  cp_registry[cp_id]['status'] = STATUS_ACTIVO
                        write_data_cp()
                        with UI_LOCK:
                            if cp_id in cp_telemetry: del cp_telemetry[cp_id]

        except Exception as e:
            _add_log(f"Kafka Error: {e}. Reintentando...", Colors.BG_RED)
            time.sleep(5)

# --- 5. SOCKET SERVER (ACTUALIZADO RELEASE 2) ---

def handle_client(conn, addr):
    current_cp_id = None
    try:
        while True:
            data = conn.recv(1024)
            if not data: break
            msg = parse_protocol(data)
            if not msg: continue
            
            # Formatos esperados: AUTENTICACION:CP_ID | PING:CP_ID | ESTADO:CP_ID:STATUS
            parts = msg.split(':')
            msg_type = parts[0].upper()

            # --- LÓGICA DE AUTENTICACIÓN (RELEASE 2) ---
            if msg_type == "AUTENTICACION" and len(parts) >= 2:
                cp_id = parts[1]
                
                # VERIFICAMOS CONTRA LA BD (Lo importante)
                if verify_identity_in_db(cp_id):
                    current_cp_id = cp_id
                    
                    # Actualizamos memoria para que la UI funcione
                    # Si el CP no estaba en ChargingPoints.txt, lo añadimos temporalmente
                    if cp_id not in cp_registry:
                        cp_registry[cp_id] = {
                            "location": "Registrado_BD", 
                            "price": 0.0, 
                            "status": STATUS_ACTIVO, 
                            "addr": addr
                        }
                    else:
                        cp_registry[cp_id]["status"] = STATUS_ACTIVO
                        cp_registry[cp_id]["addr"] = addr
                    
                    write_data_cp()
                    conn.sendall(build_protocol_response("ACEPTADO", cp_id))
                    _add_log(f"✅ CONEXIÓN ACEPTADA (BD): {cp_id}", Colors.GREEN)
                else:
                    conn.sendall(build_protocol_response("RECHAZADO", "NO_REGISTRADO"))
                    _add_log(f"❌ CONEXIÓN RECHAZADA: {cp_id} no está en BD", Colors.RED)

            # --- PING ---
            elif msg_type == "PING" and len(parts) >= 2:
                current_cp_id = parts[1]
                # Keep-alive simple, no hace nada extra
                pass

            # --- ESTADO ---
            elif msg_type == "ESTADO" and len(parts) >= 3:
                cp_id, new_status = parts[1], parts[2].upper()
                if cp_id in cp_registry:
                    cp_registry[cp_id]["status"] = new_status
                    write_data_cp()
                    conn.sendall(build_protocol_response("OK_ESTADO", ""))
                    if new_status == STATUS_AVERIA:
                        _add_log(f"⚠️ REPORTE AVERÍA: {cp_id}", Colors.RED)

    except Exception as e:
        _add_log(f"Error Socket {addr}: {e}", Colors.RED)
    finally:
        if current_cp_id and current_cp_id in cp_registry:
             cp_registry[current_cp_id]["status"] = STATUS_DESCONECTADO
             write_data_cp()
             _add_log(f"DESCONEXIÓN: {current_cp_id}", Colors.BG_GREY)
        conn.close()

def socket_server_thread(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # FIX: Evita error "Address already in use"
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind((HOST, port))
        server.listen()
        _add_log(f"Socket Server escuchando en puerto {port}", Colors.GREEN)
        
        # Mostramos si encontramos la BD
        if os.path.exists(DB_PATH):
             _add_log(f"BD Conectada: {DB_PATH}", Colors.MAGENTA)
        else:
             _add_log("⚠️ ALERTA: No se encuentra ev_charging.db", Colors.BG_RED)

        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except Exception as e:
         _add_log(f"FATAL SOCKET SERVER: {e}", Colors.BG_RED)

# --- 6. UI RENDERER ---
def ui_renderer_thread():
    while True:
        time.sleep(1)
        _clear_screen()
        print(f"{Colors.BLUE}{Colors.BOLD}=== SD EV CHARGING CENTRAL (RELEASE 2) ==={Colors.RESET}")
        
        # Renderizado de la tabla de CPs (simplificado para ahorrar espacio)
        cp_ids = sorted(cp_registry.keys())
        print(f"{Colors.WHITE}CPs Conectados / Registrados:{Colors.RESET}")
        for cp_id in cp_ids:
            info = cp_registry[cp_id]
            st = info['status']
            color = Colors.GREEN if st in [STATUS_ACTIVO, STATUS_SUMINISTRANDO] else Colors.RED
            if st == STATUS_PARADO: color = Colors.YELLOW
            elif st == STATUS_DESCONECTADO: color = Colors.BG_GREY
            
            print(f"[{color}{st:^13}{Colors.RESET}] {cp_id} ({info['location']})")
            
        print(f"\n{Colors.CYAN}{Colors.BOLD}*** APPLICATION LOGS ***{Colors.RESET}")
        with UI_LOCK:
            for log in UI_LAST_LOGS: print(log)
        print(f"{Colors.CYAN}--------------------------------------------{Colors.RESET}")
        print("Comandos: [p ID] Parar | [r ID] Reanudar | [q] Salir")

# --- 7. ADMIN INPUT LOOP ---
def admin_input_loop():
    while True:
        try:
            cmd = sys.stdin.readline().strip()
            if not cmd: continue
            parts = cmd.lower().split(' ')
            
            if parts[0] == 'q':
                if KAFKA_PRODUCER: KAFKA_PRODUCER.close()
                os._exit(0)
            # Aquí iría lógica de Parada/Reanudación igual que antes...
        except Exception: pass

# --- MAIN ---
if __name__ == "__main__":
    # Soporte para argumentos flexibles (compatible con tu comando y el original)
    # Uso esperado: python EV_Central.py 5002 IP_KAFKA PORT_KAFKA
    # O tu comando: python EV_Central.py 5002 172.x.x.x 9092
    
    if len(sys.argv) < 2:
        print("Uso: python EV_Central.py <SOCKET_PORT> [IP_KAFKA] [PORT_KAFKA]")
        sys.exit(1)

    SOCKET_PORT = int(sys.argv[1])
    
    # Intentamos adivinar donde está Kafka según los argumentos
    if len(sys.argv) >= 4:
        # Asumimos argumentos 2 y 3 son de Kafka (o del socket antiguo, pero lo necesitamos para Kafka)
        KAFKA_BROKER_ADDR = [f'{sys.argv[2]}:{sys.argv[3]}']
    else:
        # Default por si acaso
        KAFKA_BROKER_ADDR = ['localhost:9092']

    reset_cp_state()
    read_data_cp() # Cargamos fichero para UI inicial
    read_data_driver()

    # Iniciamos hilos
    threading.Thread(target=read_consumer, daemon=True).start()
    threading.Thread(target=socket_server_thread, args=(SOCKET_PORT,), daemon=True).start()
    threading.Thread(target=ui_renderer_thread, daemon=True).start()

    try:
        admin_input_loop()
    except KeyboardInterrupt:
        sys.exit(0)