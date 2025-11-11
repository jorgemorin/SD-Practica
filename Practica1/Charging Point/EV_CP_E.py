# --- Imports ---
import socket
import sys
import threading
import json
import time
from kafka import KafkaProducer, KafkaConsumer
import os
from io import StringIO

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

# --- 1. Variables Globales (Lógica) ---
KAFKA_ADDR = ()
ENGINE_LISTEN_ADDR = ()
simular_averia = False # Flag de estado de avería
kafka_producer = None # Productor persistente para telemetría
cp_id = "CP_SIN_ID" 
charge_request_pending = False
precio_kwh = 0.0
is_paused = False # Flag de pausa administrativa

# Estado de la sesión de carga actual
current_charge = {
    "active": False,
    "driver_id": None,
    "start_time": None,
    "kwh_consumed": 0.0
}

# --- 2. Globales para la UI ---
UI_LOCK = threading.Lock() # Protege logs y UI_MONITOR_CLIENT
UI_KAFKA_ADDR = "N/A"
UI_MONITOR_ADDR = "N/A"
UI_LAST_LOGS = []
UI_MONITOR_CLIENT = "N/A" # IP:Puerto del monitor conectado
_original_print = print # Guardar print original

def _add_log(message: str):
    """Añade un mensaje al log de la UI (thread-safe)."""
    with UI_LOCK:
        log_entry = f"{time.strftime('%H:%M:%S')} {message}"
        UI_LAST_LOGS.append(log_entry)
        if len(UI_LAST_LOGS) > 5:
            UI_LAST_LOGS.pop(0)

def hijacked_print(*args, **kwargs):
    """Redirige todos los 'print' al log de la UI."""
    output_buffer = StringIO()
    _original_print(*args, file=output_buffer, **kwargs)
    message = output_buffer.getvalue().strip()
    output_buffer.close()
    if message: _add_log(message)

print = hijacked_print # Sobrescribir print global

# --- 4. Lógica de Negocio ---

def create_kafka_producer(kafka_ip_port_str):
    """Inicializa el productor de Kafka para telemetría."""
    global kafka_producer
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_ip_port_str, 
            api_version=(4, 1, 0), 
            value_serializer=lambda v: v.encode('utf-8')
        )
        _original_print(f"{Colors.GREEN}[INFO] Productor Kafka OK{Colors.RESET}")
    except Exception as e:
        _original_print(f"{Colors.RED}[ERROR] Fallo Productor Kafka: {e}{Colors.RESET}")

def _trigger_failure_state():
    """Función centralizada para manejar un estado de avería."""
    global simular_averia, current_charge, kafka_producer, cp_id, precio_kwh, is_paused
    
    if simular_averia: return # Evitar ejecuciones múltiples
    
    simular_averia = True
    print(f"{Colors.RED}[!!!] ESTADO DE AVERÍA ACTIVADO.{Colors.RESET}")
    
    # Si estaba cargando o pausado, interrumpir y enviar TICKET de avería
    if current_charge["active"] or is_paused:
        driver = current_charge['driver_id']
        print(f"{Colors.RED}[!!!] Interrumpiendo carga de {driver} por avería.{Colors.RESET}")
        
        current_charge["active"] = False
        is_paused = False 
        
        kwh, cost = round(current_charge['kwh_consumed'], 2), round(current_charge['kwh_consumed'] * precio_kwh, 2)
        
        if kafka_producer and driver:
             try:
                kafka_producer.send('CPTelemetry', f"TICKET:{cp_id}:{driver}:{kwh}:{cost}:AVERIA")
             except Exception as e:
                print(f"{Colors.RED}Error enviando TICKET de avería por Kafka: {e}{Colors.RESET}")
        
        current_charge["driver_id"] = None

def start_supply_simulation_thread():
    """Simula el consumo (KWh) cada segundo y envía telemetría."""
    global current_charge, kafka_producer, cp_id, precio_kwh, is_paused
    while True:
        # Solo sumar si está activo, no pausado y no averiado
        if current_charge["active"] and kafka_producer and not is_paused and not simular_averia:
            current_charge["kwh_consumed"] += 0.01
            kwh = round(current_charge['kwh_consumed'], 2)
            cost = round(kwh * precio_kwh, 2)
            try:
                # Envía telemetría a la Central
                kafka_producer.send('CPTelemetry', f"SUMINISTRANDO:{cp_id}:{kwh}:{cost}")
            except Exception: pass
            time.sleep(1) 
        else:
            time.sleep(1)

def user_interface_thread():
    """Bucle principal de UI y entrada de usuario (teclas)."""
    global simular_averia, charge_request_pending, current_charge, cp_id, precio_kwh, is_paused
    
    while True:
        try:
            _clear_screen()
            with UI_LOCK:
                active, pending = current_charge["active"], charge_request_pending
                driver, kwh = current_charge["driver_id"], round(current_charge["kwh_consumed"], 2)
                cost, averia = round(kwh * precio_kwh, 2), simular_averia
                paused, mon_client = is_paused, UI_MONITOR_CLIENT
                logs = list(UI_LAST_LOGS)
            
            _original_print(f"{Colors.CYAN}{Colors.BOLD}=== EV ENGINE (CP: {cp_id}) ==={Colors.RESET}")
            _original_print(f"Kafka: {UI_KAFKA_ADDR} | Monitor: {UI_MONITOR_ADDR} | Cliente: {mon_client}")
            _original_print("----------------------------------------")
            _original_print(f"{Colors.BOLD}ESTADO: {Colors.RESET}", end="")
            
            if paused:
                _original_print(f"{Colors.MAGENTA}{Colors.BOLD} (PAUSADO POR CENTRAL) {Colors.RESET}", end="")

            if active: _original_print(f"{Colors.GREEN} CARGANDO (Driver: {driver}){Colors.RESET}")
            elif pending: _original_print(f"{Colors.YELLOW} SOLICITUD (Driver: {driver}) -> Pulse 'e'{Colors.RESET}")
            elif averia: _original_print(f"{Colors.RED} AVERIADO (Monitor desconectado o fallo){Colors.RESET}")
            else: _original_print(f"{Colors.CYAN} ESPERANDO...{Colors.RESET}")

            _original_print(f"{Colors.BOLD}AVERÍA: {Colors.RESET}", end="")
            _original_print(f"{Colors.RED}DETECTADA{Colors.RESET}" if averia else f"{Colors.GREEN}OK{Colors.RESET}")
            _original_print("----------------------------------------")
            if active or pending or kwh > 0:
                 _original_print(f" > Consumo: {Colors.CYAN}{kwh:.2f} KWh{Colors.RESET} | Importe: {Colors.YELLOW}{cost:.2f} €{Colors.RESET}")
                 _original_print("----------------------------------------")

            for log in logs: _original_print(f" {log}")
            _original_print("----------------------------------------")
            _original_print("[a] Simular Avería | [r] Resolver Avería | [e] Enchufar | [d] Desenchufar")
            
            comando = input(f"\n{Colors.BOLD}> {Colors.RESET}").strip().lower()
            
            if comando == 'a':
                # Simular avería manualmente
                _trigger_failure_state()

            elif comando == 'r':
                # Resolver avería manualmente
                simular_averia = False
                print(f"{Colors.GREEN}[OK] AVERÍA RESUELTA (Monitor debe reconectarse).{Colors.RESET}")

            elif comando == 'e':
                # Simular "enchufar"
                if charge_request_pending and not is_paused and not simular_averia:
                    charge_request_pending = False
                    current_charge["active"] = True
                    print(f"{Colors.GREEN}[>>>] ENCHUFADO. Suministrando...{Colors.RESET}")
                elif is_paused:
                     print(f"{Colors.YELLOW}[!] No se puede enchufar mientras está PAUSADO.{Colors.RESET}")
                elif simular_averia:
                     print(f"{Colors.RED}[!] No se puede enchufar, el CP está AVERIADO.{Colors.RESET}")

            elif comando == 'd':
                # Simular "desenchufar"
                if current_charge["active"] or (is_paused and current_charge["driver_id"] is not None):
                    print(f"{Colors.MAGENTA}[<<<] DESENCHUFADO.{Colors.RESET}")
                    kwh, cost = round(current_charge['kwh_consumed'], 2), round(current_charge['kwh_consumed'] * precio_kwh, 2)
                    # Enviar TICKET final
                    if kafka_producer and current_charge['driver_id']:
                         kafka_producer.send('CPTelemetry', f"TICKET:{cp_id}:{current_charge['driver_id']}:{kwh}:{cost}")
                    current_charge["active"] = False
                    current_charge["driver_id"] = None
                    is_paused = False 

        except (EOFError, KeyboardInterrupt): break

def start_kafka_consumer_thread():
    """Escucha comandos de la Central (START, PAUSE, RESUME)."""
    global current_charge, cp_id, charge_request_pending, is_paused
    try:
        consumer = KafkaConsumer('commands_to_cp', 
                                 bootstrap_servers=f"{KAFKA_ADDR[0]}:{KAFKA_ADDR[1]}", 
                                 auto_offset_reset='latest', 
                                 api_version=(4, 1, 0))
        print(f"{Colors.GREEN}[INFO] Escuchando 'commands_to_cp'...{Colors.RESET}")
        for message in consumer:
            msg = message.value.decode('utf-8')
            parts = msg.split(':')
            if len(parts) < 2 or parts[1] != cp_id: continue # Filtrar mensajes para este CP

            cmd = parts[0]
            if cmd == "START" and len(parts) == 3 and not current_charge["active"] and not simular_averia:
                current_charge.update({"driver_id": parts[2], "kwh_consumed": 0.0})
                charge_request_pending = True
                print(f"{Colors.YELLOW}[!] SOLICITUD DE CARGA de {parts[2]}. Pulse 'e'.{Colors.RESET}")
            elif cmd == "STOP":
                if current_charge["active"] or charge_request_pending:
                    current_charge["active"] = charge_request_pending = False
                    current_charge["driver_id"] = None
                    print(f"{Colors.MAGENTA}[!] STOP recibido de Central.{Colors.RESET}")
            elif cmd == "PAUSE":
                is_paused = True
                print(f"{Colors.MAGENTA}[!] PAUSA ADMINISTRATIVA recibida.{Colors.RESET}")
            elif cmd == "RESUME":
                is_paused = False
                print(f"{Colors.GREEN}[!] RESUME recibido. Operativo.{Colors.RESET}")

    except Exception as e: print(f"{Colors.RED}[ERROR KAFKA CONSUMER] {e}{Colors.RESET}")

def load_config(filename="engine.conf"):
    """Carga configuración (ID, Precio) desde engine.conf."""
    config = {}
    try:
        base = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(base, filename), 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    k, v = line.strip().split('=', 1)
                    config[k.strip()] = int(v.strip()) if 'PUERTO' in k else v.strip()
        return config
    except Exception as e:
        _original_print(f"{Colors.RED}[ERROR] Config {filename}: {e}{Colors.RESET}")
        return None

def handle_monitor_connection(conn, addr):
    """Maneja la conexión socket del Monitor (local)."""
    global simular_averia, is_paused, UI_MONITOR_CLIENT
    
    with UI_LOCK:
        if UI_MONITOR_CLIENT != "N/A":
            print(f"{Colors.YELLOW}Rechazando conexión de monitor duplicado desde {addr}{Colors.RESET}")
            conn.close()
            return
        UI_MONITOR_CLIENT = str(addr)
    
    try:
        while True:
            if not conn.recv(1024): 
                break # Monitor desconectado
            
            # Responder al HEALTH_CHECK
            if simular_averia: 
                conn.sendall(b"KO: AVERIA SIMULADA")
            elif is_paused: 
                conn.sendall(b"OK:PAUSED")
            else: 
                conn.sendall(b"OK")
    except (socket.timeout, ConnectionResetError, BrokenPipeError):
        pass # Monitor desconectado
    except Exception as e:
        print(f"Error en Hilo Monitor: {e}")
    finally:
        with UI_LOCK: 
            UI_MONITOR_CLIENT = "N/A"
        
        # Si el monitor se desconecta, disparamos la avería interna
        print(f"{Colors.RED}[!!!] Conexión del Monitor perdida. Disparando avería...{Colors.RESET}")
        _trigger_failure_state()
        conn.close()

def start_monitor_server():
    """Abre un socket server para que el Monitor se conecte."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(ENGINE_LISTEN_ADDR)
        s.listen()
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_monitor_connection, args=(conn, addr), daemon=True).start()
    except Exception as e: _add_log(f"Monitor Server Error: {e}")

def __main__():
    global KAFKA_ADDR, ENGINE_LISTEN_ADDR, cp_id, precio_kwh, UI_KAFKA_ADDR, UI_MONITOR_ADDR
    
    if len(sys.argv) != 3: return _original_print(f"Uso: python EV_CP_E.py <IP_KAFKA> <PUERTO_KAFKA>")
    KAFKA_ADDR = (sys.argv[1], int(sys.argv[2]))
    
    conf = load_config()
    if not conf: return
    ENGINE_LISTEN_ADDR = (conf['IP_ESCUCHA'], conf['PUERTO_ESCUCHA'])
    cp_id, precio_kwh = conf['CP_ID'], float(conf['PRECIO_KWH'])
    UI_KAFKA_ADDR, UI_MONITOR_ADDR = f"{KAFKA_ADDR[0]}:{KAFKA_ADDR[1]}", f"{ENGINE_LISTEN_ADDR[0]}:{ENGINE_LISTEN_ADDR[1]}"

    create_kafka_producer(UI_KAFKA_ADDR)
    if not kafka_producer: return

    # Iniciar hilos de trabajo
    threading.Thread(target=start_monitor_server, daemon=True).start()
    threading.Thread(target=start_kafka_consumer_thread, daemon=True).start()
    threading.Thread(target=start_supply_simulation_thread, daemon=True).start()
    
    try:
        user_interface_thread() # Hilo principal se convierte en UI
    except KeyboardInterrupt: 
        pass
    finally:
        print = _original_print # Restaurar print original
        if kafka_producer: kafka_producer.close()

if __name__ == "__main__":
    __main__()