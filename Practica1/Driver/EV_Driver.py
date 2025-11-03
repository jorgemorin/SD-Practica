# -*- coding: utf-8 -*-
# EV_Driver: Driver application logic for the EVCharging system.
# Implements Request/Reply via Kafka and file-based automatic service requests (Punto 3).

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
import sys
import threading
import os # NUEVO: Para limpiar la pantalla
from typing import List, Dict, Any, Optional

# --- 0. CONSTANTES VISUALES (NUEVO) ---
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BLUE = "\033[94m"
GRAY = "\033[90m"

# --- 1. CONFIGURATION AND CONSTANTS ---
KAFKA_REQUEST = 'DriverRequest'
KAFKA_RESPONSE = 'DriverResponse'
KAFKA_TELEMETRY = 'CPTelemetry' # NEW: Driver must listen to telemetry for the current CP (Punto 8)

# Kafka Timeout Optimization (ms)
FAST_INIT_TIMEOUT = 10001 
DEFAULT_GET_TIMEOUT = 100   # 10s timeout for send acknowledgment
PAUSE_AFTER_SERVICE = 4    # Seconds to wait after service conclusion (Punto 195)

# Consumer Thread Config (Settings retained for stability)
CONSUMER_SESSION_TIMEOUT = 10000 
CONSUMER_HEARTBEAT = 3000

# --- 2. GLOBAL STATE MANAGEMENT ---
# Estado de lógica de negocio
RESPONSE_STATE: Dict[str, Optional[Any]] = {"decision": None, "cp_id": None}
RESPONSE_LOCK = threading.Lock()
SERVICE_CONCLUSION_EVENT = threading.Event()
CURRENT_CHARGING_CP: Optional[str] = None # NEW: Track the CP the driver is connected to

# Estado para el hilo de procesamiento de archivos
FILE_PROCESSOR_THREAD: Optional[threading.Thread] = None

# NUEVO: Estado para la interfaz visual (TUI)
UI_STATUS = f"{YELLOW}Iniciando...{RESET}"
UI_LAST_NOTIFICATION = "---"
UI_LAST_TELEMETRY = "---"
UI_LAST_TICKET = "---" # NUEVO: Para el último ticket recibido
UI_LOCK = threading.Lock() # Lock para proteger las variables de la UI

# --- 3. KAFKA PRODUCER/CONSUMER CORE FUNCTIONS ---

def _create_producer(kafka_broker: List[str]) -> Optional[KafkaProducer]:
    """Initializes and returns a KafkaProducer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            api_version=(4, 1, 0),
            request_timeout_ms=FAST_INIT_TIMEOUT 
        )
        return producer
    except Exception as e:
        print(f"[KAFKA PRODUCER] ERROR: Failed to create producer: {e}") # Error de sistema, se mantiene el print
        return None

def _send_message_sync(producer: KafkaProducer, topic: str, message: str, timeout: int) -> bool:
    """Sends a message synchronously and waits for acknowledgment."""
    global UI_LAST_NOTIFICATION
    try:
        future = producer.send(topic, value=message.encode('utf-8'))
        future.get(timeout=timeout)
        
        # Actualizar UI en lugar de imprimir
        with UI_LOCK:
            UI_LAST_NOTIFICATION = f"Enviado: {message}"
        
        return True
    except Exception as e:
        # Actualizar UI en lugar de imprimir
        with UI_LOCK:
            UI_LAST_NOTIFICATION = f"{RED}ERROR: Fallo al enviar mensaje: {e}{RESET}"
        return False
    finally:
        producer.close()

def request_service(driver_id: str, cp_id: str, kafka_broker: List[str]):
    """Sends a CP service request message (REQUEST:DRIVER_ID:CP_ID)."""
    global CURRENT_CHARGING_CP
    CURRENT_CHARGING_CP = cp_id # Set the requested CP
    
    with UI_LOCK:
        global UI_STATUS
        UI_STATUS = f"{YELLOW}Solicitando CP {cp_id}...{RESET}"
        
    producer = _create_producer(kafka_broker)
    if producer:
        message = f"REQUEST:{driver_id}:{cp_id}"
        _send_message_sync(producer, KAFKA_REQUEST, message, DEFAULT_GET_TIMEOUT)

# --- FUNCIONES 'send_stop_signal' y 'handle_charging_session' ELIMINADAS ---
# El Driver ya no envía la señal de STOP. Se asume que Engine lo gestiona.


# --- 4. SESSION AND CONSUMER LOGIC ---

def receive_responses(kafka_broker: List[str], driver_id: str):
    """Consumer thread: listens for responses (KAFKA_RESPONSE) and Telemetry (KAFKA_TELEMETRY)."""
    consumer = None
    consumer_group = f'ev_driver_group_{driver_id}'
    
    # Añadido UI_LAST_TICKET
    global UI_LAST_NOTIFICATION, UI_LAST_TELEMETRY, CURRENT_CHARGING_CP, UI_LAST_TICKET

    try:
        consumer = KafkaConsumer(
            KAFKA_RESPONSE, 
            KAFKA_TELEMETRY, # Listen to telemetry (Punto 8)
            bootstrap_servers=kafka_broker,
            auto_offset_reset='latest', 
            enable_auto_commit=True,
            group_id=consumer_group,
            api_version=(4, 1, 0),
            request_timeout_ms=FAST_INIT_TIMEOUT,
            session_timeout_ms=CONSUMER_SESSION_TIMEOUT,
            heartbeat_interval_ms=CONSUMER_HEARTBEAT
        )
        print("[KAFKA CONSUMER] Successfully connected (esto solo se ve al inicio).")
        
        for message in consumer:
            response_str = message.value.decode('utf-8')
            
            if message.topic == KAFKA_RESPONSE:
                parts = response_str.split(':')
                if len(parts) >= 3 and parts[1] == driver_id: # Filter by Driver ID
                    decision = parts[0]
                    cp_id_response = parts[2]

                    with RESPONSE_LOCK:
                        RESPONSE_STATE["decision"] = decision
                        RESPONSE_STATE["cp_id"] = cp_id_response
                    
                    # Actualizar UI en lugar de imprimir
                    with UI_LOCK:
                        if decision == 'ACEPTADO':
                            UI_LAST_NOTIFICATION = f"{GREEN}ACEPTADO en CP {cp_id_response}{RESET}"
                        elif decision == 'RECHAZADO':
                            UI_LAST_NOTIFICATION = f"{RED}RECHAZADO en CP {cp_id_response}{RESET}"
                        
                    if decision == 'RECHAZADO':
                        SERVICE_CONCLUSION_EVENT.set()
                        CURRENT_CHARGING_CP = None

                elif parts[0] == "TICKET" and len(parts) >= 5 and parts[2] == driver_id:  
                    cp_id_ticket = parts[1]  
                    consumo = parts[3]  
                    importe = parts[4]
                    
                    # --- CAMBIO AQUÍ ---
                    # Guardamos el ticket en su variable dedicada
                    with UI_LOCK:
                        if len(parts)==6 and parts[5]=="AVERIA":
                            UI_LAST_TICKET = f"{RED}[TICKET AVERIA] CP {cp_id_ticket}: {consumo}Kwh, {importe}${RESET}"
                        else:
                            UI_LAST_TICKET = f"{GREEN}[RECIBO] CP {cp_id_ticket}: {consumo}Kwh, {importe}${RESET}"
                    
                    with RESPONSE_LOCK:  
                        RESPONSE_STATE["decision"] = 'TICKET'  
                        RESPONSE_STATE["cp_id"] = cp_id_ticket  
                    
                    SERVICE_CONCLUSION_EVENT.set()  
                    CURRENT_CHARGING_CP = None

                elif parts[0] == "CP_LIST":
                    # Actualizar UI en lugar de imprimir
                    with UI_LOCK:
                        # --- CAMBIO DE COLOR ---
                        UI_LAST_NOTIFICATION = f"CPs Activos: {CYAN}{response_str[8:]}{RESET}"

            elif message.topic == KAFKA_TELEMETRY:
                tele_parts = response_str.split(':')
                if len(tele_parts) >= 3 and tele_parts[0] == CURRENT_CHARGING_CP:
                    cp_id, consumo, importe = tele_parts[0], tele_parts[1], tele_parts[2]
                    
                    # Actualizar UI en lugar de imprimir
                    with UI_LOCK:
                        UI_LAST_TELEMETRY = f"Consumo: {consumo}Kw | Importe: {importe}$"

    except Exception as e:
        print(f"[KAFKA CONSUMER] ERROR: Consumer thread failed: {e}") # Error de sistema
    finally:
        if consumer is not None:
            consumer.close()

# --- 5. FILE PROCESSING LOGIC (Punto 3, 195) ---

def process_file_requests(file_name: str, driver_id: str, kafka_broker: List[str]):
    """Reads CP IDs from file and sends requests sequentially, waiting for conclusion."""
    
    global UI_LAST_NOTIFICATION, FILE_PROCESSOR_THREAD
    
    try:
        with open(file_name, 'r') as file:
            cp_ids = [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        with UI_LOCK:
            UI_LAST_NOTIFICATION = f"{RED}ERROR: Fichero '{file_name}' no encontrado.{RESET}"
        return

    with UI_LOCK:
        UI_LAST_NOTIFICATION = f"Iniciando secuencia de {len(cp_ids)} solicitudes."
    
    for i, cp_id in enumerate(cp_ids):
        with UI_LOCK:
            UI_LAST_NOTIFICATION = f"Solicitud {i+1}/{len(cp_ids)} para CP {cp_id}"
        
        SERVICE_CONCLUSION_EVENT.clear()
        request_service(driver_id, cp_id, kafka_broker)
        
        with UI_LOCK:
            UI_LAST_NOTIFICATION = f"Esperando conclusion del servicio (CP {cp_id})..."
        
        SERVICE_CONCLUSION_EVENT.wait() 
        
        # El hilo principal (main_menu_loop) gestionará la espera de 'ACEPTADO'
        # y el bloqueo hasta recibir 'TICKET'.
        # Este hilo (process_file_requests) solo espera la conclusión final.
        
        with UI_LOCK:
            UI_LAST_NOTIFICATION = f"Servicio {i+1} concluido. Pausa de {PAUSE_AFTER_SERVICE}s."
        
        time.sleep(PAUSE_AFTER_SERVICE)

    with UI_LOCK:
        UI_LAST_NOTIFICATION = "Secuencia de fichero finalizada."
        
    FILE_PROCESSOR_THREAD = None

def show_cp_available(driver_id: str, kafka_broker: List[str]):
    with UI_LOCK:
        global UI_LAST_NOTIFICATION
        UI_LAST_NOTIFICATION = "Solicitando lista de CPs activos..."
        
    producer = _create_producer(kafka_broker)
    if producer:
        message = f"CP_REQUEST:{driver_id}"
        _send_message_sync(producer, KAFKA_REQUEST, message, DEFAULT_GET_TIMEOUT)
    

# --- 6. UI Y BUCLE PRINCIPAL (NUEVO) ---

def clear_screen():
    """Limpia la pantalla de la consola."""
    os.system('cls' if os.name == 'nt' else 'clear')

def render_screen(driver_id: str, kafka_broker: List[str]):
    """Dibuja la interfaz visual en la consola."""
    clear_screen()
    
    # Obtener estado de forma segura
    with UI_LOCK:
        status = UI_STATUS
        notification = UI_LAST_NOTIFICATION
        telemetry = UI_LAST_TELEMETRY
        last_ticket = UI_LAST_TICKET
        current_cp = CURRENT_CHARGING_CP
        is_file_processing = FILE_PROCESSOR_THREAD and FILE_PROCESSOR_THREAD.is_alive()

    print(f"{CYAN}{'═'*60}{RESET}")
    print(f"{BOLD}{' PANEL DE CONDUCTOR EV ':^60}{RESET}")
    print(f"{CYAN}{'═'*60}{RESET}")
    print(f"{BOLD}DRIVER ID:{RESET} {driver_id}")
    print(f"{BOLD}KAFKA:{RESET}   {kafka_broker[0]}")
    print(f"{CYAN}{'-'*60}{RESET}")
    print(f"{BOLD}ESTADO:{RESET}  {status}")
    print(f"{CYAN}{'-'*60}{RESET}")
    
    # Mostrar telemetría solo si está cargando
    if current_cp and "Cargando" in status:
        print(f"{BOLD}CP ACTUAL:{RESET} {current_cp}")
        print(f"{BOLD}TELEMETRIA:{RESET} {YELLOW}{telemetry}{RESET}")
    elif is_file_processing:
         print(f"{BOLD}MODO:{RESET}      {CYAN}Procesando fichero...{RESET}")
    else:
        print(f"{BOLD}CP ACTUAL:{RESET} ---")
        print(f"{BOLD}TELEMETRIA:{RESET} ---")
    
    # Nueva sección para el Ticket
    print(f"{CYAN}{'-'*60}{RESET}")
    print(f"{BOLD}ULTIMO TICKET:{RESET}")
    print(f"  {last_ticket}")
    
    print(f"{CYAN}{'-'*60}{RESET}")
    print(f"{BOLD}NOTIFICACION:{RESET}")
    print(f"  {notification}")
    print(f"{CYAN}{'═'*60}{RESET}")

def main_menu_loop(driver_id: str, kafka_broker: List[str]):
    """Main application loop to display the menu and handle user input."""
    global FILE_PROCESSOR_THREAD, CURRENT_CHARGING_CP
    global UI_STATUS, UI_LAST_NOTIFICATION, UI_LAST_TELEMETRY, UI_LAST_TICKET

    is_menu_active = True

    while True:
        
        # 1. Comprobar estado de Kafka (Lógica de negocio original)
        with RESPONSE_LOCK:
            decision = RESPONSE_STATE["decision"]
            cp_id_response = RESPONSE_STATE["cp_id"]
            RESPONSE_STATE["decision"] = None # Reset decision status
        
        # 2. Actualizar UI basada en la decisión de Kafka (Lógica de presentación)
        with UI_LOCK:
            if decision == 'ACEPTADO':
                UI_STATUS = f"{GREEN}Cargando en CP {cp_id_response}{RESET}"
                UI_LAST_TELEMETRY = "Iniciando..."
                # --- CAMBIO AQUÍ ---
                UI_LAST_NOTIFICATION = f"Vehiculo enchufado. Recibiendo telemetria."
                
            elif decision == 'RECHAZADO':
                UI_STATUS = f"{YELLOW}En espera{RESET}"
                UI_LAST_NOTIFICATION = f"{RED}RECARGA RECHAZADA en CP {cp_id_response}{RESET}"
                CURRENT_CHARGING_CP = None
                SERVICE_CONCLUSION_EVENT.set()
            
            elif decision == 'TICKET':
                UI_STATUS = f"{YELLOW}En espera{RESET}"
                UI_LAST_NOTIFICATION = f"{GREEN}Recarga finalizada en CP {cp_id_response}. Ticket recibido.{RESET}"
                CURRENT_CHARGING_CP = None
                SERVICE_CONCLUSION_EVENT.set()

        # 3. Comprobar si estamos ocupados (Lógica de negocio original)
        is_busy = (FILE_PROCESSOR_THREAD and FILE_PROCESSOR_THREAD.is_alive()) or CURRENT_CHARGING_CP is not None
        
        # 4. Actualizar estado de la UI (Lógica de presentación)
        with UI_LOCK:
            if is_busy:
                is_menu_active = False # Ocultar menú
                if CURRENT_CHARGING_CP and not "Cargando" in UI_STATUS:
                     UI_STATUS = f"{GREEN}Cargando en CP {CURRENT_CHARGING_CP}{RESET}"
                elif FILE_PROCESSOR_THREAD and FILE_PROCESSOR_THREAD.is_alive():
                    UI_STATUS = f"{CYAN}Modo automatico (fichero) activo...{RESET}"
            else:
                if not is_menu_active: # Si acabamos de dejar de estar ocupados
                    UI_LAST_TELEMETRY = "---" # Limpiar telemetría (pero NO el ticket)
                is_menu_active = True # Mostrar menú
                UI_STATUS = f"{YELLOW}Menu principal{RESET}"

        # 5. Renderizar la pantalla
        render_screen(driver_id, kafka_broker)

        # 6. Mostrar menú y pedir input (si no está ocupado)
        if is_menu_active:
            print(f"{BOLD}Selecciona una opcion:{RESET}")
            print(" [1] Solicitar suministro de CP (Manual)")
            print(" [2] Leer fichero de servicios (Auto)")
            print(" [3] Solicitar el listado de CPs Activos")
            # --- OPCION [s] ELIMINADA ---
            
            opcion = input(f"{BOLD}> {RESET}")

            if opcion == '1':
                try:
                    cp_id_input = str(input(f"{BOLD}  ID del Charging Point: {RESET}"))
                except Exception:
                    with UI_LOCK:
                        UI_LAST_NOTIFICATION = f"{RED}ID de Charging Point invalido.{RESET}"
                    continue
                request_service(driver_id, cp_id_input, kafka_broker)
            
            elif opcion == '2':
                file_name = input(f"{BOLD}  Nombre del archivo (ej: servicios.txt): {RESET}")
                FILE_PROCESSOR_THREAD = threading.Thread(target=process_file_requests, args=(file_name, driver_id, kafka_broker), daemon=True)
                FILE_PROCESSOR_THREAD.start()

            elif opcion == '3':
                show_cp_available(driver_id, kafka_broker)
            
            # --- BLOQUE elif opcion == 's' ELIMINADO ---

            else:
                with UI_LOCK:
                    UI_LAST_NOTIFICATION = f"{RED}Opcion invalida.{RESET}"
            
            time.sleep(0.1) # Pequeña pausa
        
        else:
            # Si está ocupado, solo refrescamos la pantalla
            # El Driver ahora es "view-only" durante la carga
            time.sleep(1) # Refresh rate de 1 segundo

def main():
    """Main execution entry point."""
    if len(sys.argv) != 4:
        print("Usage: python EV_Driver.py <BROKER_IP> <BROKER_PORT> <DRIVER_ID>")
        return
        
    KAFKA_BROKER = [f'{sys.argv[1]}:{sys.argv[2]}']
    DRIVER_ID = sys.argv[3]

    # Start the response listener thread (must be started first)
    consumer_thread = threading.Thread(target=receive_responses, args=(KAFKA_BROKER, DRIVER_ID), daemon=True)
    consumer_thread.start()

    try:
        main_menu_loop(DRIVER_ID, KAFKA_BROKER)
    except KeyboardInterrupt:
        clear_screen()
        print("\nSaliendo...")
    except Exception as e:
        clear_screen()
        print(f"\n{RED}ERROR INESPERADO EN EL BUCLE PRINCIPAL:{RESET} {e}")
        print("Por favor, reinicie la aplicacion.")

if __name__ == "__main__":
    main()