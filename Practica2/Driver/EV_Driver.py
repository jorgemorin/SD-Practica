# -*- coding: utf-8 -*-
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
import sys
import threading
<<<<<<< HEAD
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
=======
import os
from typing import List, Dict, Any, Optional

# --- 0. VISUALS ---
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
>>>>>>> origin/Dani

# --- 1. CONFIGURATION AND CONSTANTS ---
KAFKA_REQUEST = 'DriverRequest'
KAFKA_RESPONSE = 'DriverResponse'
KAFKA_TELEMETRY = 'CPTelemetry'
FAST_INIT_TIMEOUT = 10001 
DEFAULT_GET_TIMEOUT = 10   
PAUSE_AFTER_SERVICE = 4
CONSUMER_SESSION_TIMEOUT = 10000 
CONSUMER_HEARTBEAT = 3000

# --- 2. GLOBAL STATE MANAGEMENT ---
<<<<<<< HEAD
# Estado de lógica de negocio
=======
# Estado de la respuesta de Central
>>>>>>> origin/Dani
RESPONSE_STATE: Dict[str, Optional[Any]] = {"decision": None, "cp_id": None}
RESPONSE_LOCK = threading.Lock() # Protege RESPONSE_STATE
SERVICE_CONCLUSION_EVENT = threading.Event() # Sincroniza el modo fichero

<<<<<<< HEAD
# Estado para el hilo de procesamiento de archivos
FILE_PROCESSOR_THREAD: Optional[threading.Thread] = None

# NUEVO: Estado para la interfaz visual (TUI)
UI_STATUS = f"{YELLOW}Iniciando...{RESET}"
UI_LAST_NOTIFICATION = "---"
UI_LAST_TELEMETRY = "---"
UI_LAST_TICKET = "---" # NUEVO: Para el último ticket recibido
UI_LOCK = threading.Lock() # Lock para proteger las variables de la UI
=======
CURRENT_CHARGING_CP: Optional[str] = None # CP al que estamos conectados
LAST_TICKET_INFO: Optional[str] = None
FILE_PROCESSOR_THREAD: Optional[threading.Thread] = None

# Productor de Kafka global y persistente
KAFKA_PRODUCER: Optional[KafkaProducer] = None
PRODUCER_LOCK = threading.Lock()
KAFKA_BROKER_ADDR: List[str] = [] 
>>>>>>> origin/Dani

# --- 3. KAFKA PRODUCER/CONSUMER CORE FUNCTIONS ---

def _initialize_producer() -> bool:
    """Inicializa el productor global de Kafka."""
    global KAFKA_PRODUCER, KAFKA_BROKER_ADDR
    if KAFKA_PRODUCER is not None:
        return True
    
    print(f"{Colors.YELLOW}[KAFKA] Intentando conectar productor global a {KAFKA_BROKER_ADDR}...{Colors.RESET}")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_ADDR,
            api_version=(4, 1, 0),
            request_timeout_ms=FAST_INIT_TIMEOUT 
        )
<<<<<<< HEAD
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
=======
        KAFKA_PRODUCER = producer
        print(f"{Colors.GREEN}[KAFKA PRODUCER] Productor global inicializado.{Colors.RESET}")
        return True
    except Exception as e:
        print(f"{Colors.RED}[KAFKA PRODUCER] ERROR: Fallo al crear productor global: {e}{Colors.RESET}")
>>>>>>> origin/Dani
        return False

def _send_message_sync(topic: str, message: str, timeout: int = DEFAULT_GET_TIMEOUT) -> bool:
    """Envía un mensaje Kafka usando el productor global (thread-safe)."""
    global KAFKA_PRODUCER
    with PRODUCER_LOCK:
        if KAFKA_PRODUCER is None:
            print(f"{Colors.RED}[KAFKA PRODUCER] ERROR: Productor no esta inicializado. Intentando reconectar...{Colors.RESET}")
            if not _initialize_producer():
                return False 
        
        try:
            print(f"{Colors.CYAN}[KAFKA SEND] Enviando: {message} al topic {topic}{Colors.RESET}")
            future = KAFKA_PRODUCER.send(topic, value=message.encode('utf-8'))
            future.get(timeout=timeout)
            print(f"{Colors.GREEN}[KAFKA SEND] Mensaje enviado con éxito.{Colors.RESET}")
            return True
        except Exception as e:
            print(f"{Colors.RED}[KAFKA PRODUCER] ERROR: Error al enviar: {e}{Colors.RESET}")
            # Resetear productor en caso de fallo
            try:
                if KAFKA_PRODUCER: KAFKA_PRODUCER.close()
            except: pass
            KAFKA_PRODUCER = None
            return False

def request_service(driver_id: str, cp_id: str):
    """Envía una solicitud de servicio (REQUEST:DRIVER_ID:CP_ID)."""
    global CURRENT_CHARGING_CP
<<<<<<< HEAD
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
=======
    CURRENT_CHARGING_CP = cp_id 
    message = f"REQUEST:{driver_id}:{cp_id}"
    _send_message_sync(KAFKA_REQUEST, message)

def send_stop_signal(driver_id: str, cp_id: str):
    """Envía la señal de STOP (simulando desenchufe manual)."""
    message = f"STOP:{driver_id}:{cp_id}" 
    _send_message_sync(KAFKA_REQUEST, message)

# --- 4. SESSION AND CONSUMER LOGIC ---
    
def receive_responses(driver_id: str):
    """Hilo consumidor de Kafka (topics Response y Telemetry)."""
>>>>>>> origin/Dani
    consumer = None
    consumer_group = f'ev_driver_group_{driver_id}'
    global LAST_TICKET_INFO, KAFKA_BROKER_ADDR
    
<<<<<<< HEAD
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
=======
    while True: # Bucle de reconexión del consumidor
        try:
            consumer = KafkaConsumer(
                KAFKA_RESPONSE, 
                KAFKA_TELEMETRY,
                bootstrap_servers=KAFKA_BROKER_ADDR,
                auto_offset_reset='latest', 
                enable_auto_commit=True,
                group_id=consumer_group,
                api_version=(4, 1, 0),
                request_timeout_ms=FAST_INIT_TIMEOUT,
                session_timeout_ms=CONSUMER_SESSION_TIMEOUT,
                heartbeat_interval_ms=CONSUMER_HEARTBEAT
            )
            print(f"{Colors.GREEN}[KAFKA CONSUMER] Conectado y escuchando.{Colors.RESET}")
            
            for message in consumer:
                response_str = message.value.decode('utf-8')
                
                
                if message.topic == KAFKA_RESPONSE:
                    parts = response_str.split(':')
                    
                    # Mensajes para este driver
                    if len(parts) >= 3 and parts[1] == driver_id:
                        decision = parts[0]
                        cp_id_response = parts[2]
                        
                        with RESPONSE_LOCK:
                            RESPONSE_STATE["decision"] = decision
                            RESPONSE_STATE["cp_id"] = cp_id_response
                        
                        if decision == 'RECHAZADO':
                            SERVICE_CONCLUSION_EVENT.set()
                            global CURRENT_CHARGING_CP
                            CURRENT_CHARGING_CP = None
                    
                    # Tickets (tienen formato distinto)
                    elif parts[0] == "TICKET" and len(parts) >= 5 and parts[2] == driver_id:  
                        cp_id_ticket = parts[1]  
                        consumo = parts[3]  
                        importe = parts[4]
                        status_msg = f"{Colors.GREEN}CP {cp_id_ticket}: {consumo} Kwh | {importe} ${Colors.RESET}"
                        
                        if len(parts)==6 and parts[5]=="AVERIA":
                              print(f"\n{Colors.RED}{Colors.BOLD}<<< TICKET POR AVERIA (CP {cp_id_ticket}) >>>{Colors.RESET}")
                              print(f"{Colors.RED}  [ERROR] El punto de carga ha sufrido una averia")
                              print(f"  Consumo: {consumo} Kwh | Importe: {importe} ${Colors.RESET}")
                              status_msg = f"{Colors.RED}AVERIA (CP {cp_id_ticket}): {consumo} Kwh | {importe} ${Colors.RESET}"
                        else:
                            print(f"\n{Colors.GREEN}{Colors.BOLD}<<< TICKET RECIBIDO (CP {cp_id_ticket}) >>>{Colors.RESET}")
                            print(f"{Colors.GREEN}  Consumo: {consumo} Kwh | Importe: {importe} ${Colors.RESET}")
                        
                        LAST_TICKET_INFO = status_msg
                        
                        with RESPONSE_LOCK:  
                            RESPONSE_STATE["decision"] = 'TICKET'  
                            RESPONSE_STATE["cp_id"] = cp_id_ticket  
                        
                        SERVICE_CONCLUSION_EVENT.set()  
                        CURRENT_CHARGING_CP = None
                    
                    # Lista de CPs
                    elif parts[0] == "CP_LIST":
                        print(f"\n{Colors.CYAN}{Colors.BOLD}<<< PUNTOS DE CARGA ACTIVOS >>>{Colors.RESET}")
                        print(f"{Colors.BOLD}  {'CP ID':<8} | {'UBICACION':<25} | {'PRECIO':<10}{Colors.RESET}")
                        print(f"  {'-'*8} | {'-'*25} | {'-'*10}")
                        
                        cp_data_str = response_str[8:] # Todo después de "CP_LIST:"
                        if not cp_data_str:
                             print(f"  {Colors.YELLOW}No hay CPs activos en este momento.{Colors.RESET}")
                        
                        try:
                            cp_items = cp_data_str.split(':')
                            for item in cp_items:
                                if '(' in item and ')@' in item:
                                    part1 = item.split('(')
                                    cp_id = part1[0]
                                    part2 = part1[1].split(')@')
                                    ubicacion = part2[0]
                                    precio = part2[1]
                                    print(f"  {Colors.GREEN}{cp_id:<8}{Colors.RESET} | {ubicacion:<25} | {Colors.YELLOW}{precio:<10}{Colors.RESET}")
                        except Exception as e:
                            print(f"  {Colors.RED}Error al parsear la lista de CPs. Datos brutos:{Colors.RESET} {cp_data_str}")
                        print(f"\n{Colors.YELLOW}Presione [Enter] para volver al menu...{Colors.RESET}")


                elif message.topic == KAFKA_TELEMETRY:
                    tele_parts = response_str.split(':')
                    # Formato: "SUMINISTRANDO:CP_ID:KWH:COST"
                    if tele_parts[0] == "SUMINISTRANDO" and len(tele_parts) >= 4 and tele_parts[1] == CURRENT_CHARGING_CP:
                        cp_id, consumo, importe = tele_parts[1], tele_parts[2], tele_parts[3]
                        # Imprime en la misma línea para telemetría en tiempo real
                        print(f"\r{Colors.CYAN}[TELEMETRY CP {cp_id}] Consumo: {consumo} Kwh | Importe: {importe} ${Colors.RESET}   ", end="")
>>>>>>> origin/Dani


<<<<<<< HEAD
def process_file_requests(file_name: str, driver_id: str, kafka_broker: List[str]):
    """Reads CP IDs from file and sends requests sequentially, waiting for conclusion."""
    
    global UI_LAST_NOTIFICATION, FILE_PROCESSOR_THREAD
    
=======
        except Exception as e:
            print(f"{Colors.RED}[KAFKA CONSUMER] ERROR: Consumer thread failed: {e}. Reintentando en 5s...{Colors.RESET}")
            if consumer is not None:
                consumer.close()
            time.sleep(5) 

# --- 5. FILE PROCESSING LOGIC ---

def process_file_requests(file_name: str, driver_id: str):
    """Procesa una lista de CPs desde un fichero, secuencialmente."""
>>>>>>> origin/Dani
    try:
        with open(file_name, 'r') as file:
            cp_ids = [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
<<<<<<< HEAD
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
=======
        print(f"{Colors.RED}[FILE PROCESSOR] ERROR: File '{file_name}' not found.{Colors.RESET}")
        return

    print(f"{Colors.MAGENTA}[FILE PROCESSOR] Iniciando secuencia de {len(cp_ids)} recargas.{Colors.RESET}")
    
    for i, cp_id in enumerate(cp_ids):
        print(f"\n{Colors.MAGENTA}[FILE PROCESSOR] --- Iniciando recarga {i+1}/{len(cp_ids)} para CP: {cp_id} ---{Colors.RESET}")
        
        SERVICE_CONCLUSION_EVENT.clear()
        
        request_service(driver_id, cp_id)
        
        print(f"{Colors.MAGENTA}[FILE PROCESSOR] Esperando fin de servicio...{Colors.RESET}")
        SERVICE_CONCLUSION_EVENT.wait()  # Bloquea hasta que se recibe TICKET o RECHAZADO

        print(f"{Colors.MAGENTA}[FILE PROCESSOR] Servicio finalizado. Pausa de {PAUSE_AFTER_SERVICE} seg...{Colors.RESET}")
        time.sleep(PAUSE_AFTER_SERVICE)

    print(f"{Colors.MAGENTA}[FILE PROCESSOR] Secuencia de fichero terminada.{Colors.RESET}")
    
    global FILE_PROCESSOR_THREAD
    FILE_PROCESSOR_THREAD = None

def show_cp_available(driver_id: str):
    """Solicita la lista de CPs activos a la Central."""
    print(f"{Colors.YELLOW}[KAFKA] Solicitando lista de CPs activos a Central...{Colors.RESET}")
    message = f"CP_REQUEST:{driver_id}"
    _send_message_sync(KAFKA_REQUEST, message)
    

# --- 6. MAIN APPLICATION LOGIC ---
def main_menu_loop(driver_id: str):
    """Bucle principal de la UI del driver."""
    global FILE_PROCESSOR_THREAD, CURRENT_CHARGING_CP, LAST_TICKET_INFO
    
    last_notification = ""

    while True:
        
        # 1. Procesar estado de Kafka
        with RESPONSE_LOCK:
            decision = RESPONSE_STATE["decision"]
            cp_id_response = RESPONSE_STATE["cp_id"]
            RESPONSE_STATE["decision"] = None # Resetear estado
            
            if decision == 'ACEPTADO':
                last_notification = f"{Colors.GREEN}RECARGA ACEPTADA en CP {cp_id_response}. Conectado. Esperando telemetría...{Colors.RESET}"
                
            elif decision == 'RECHAZADO':
                last_notification = f"{Colors.RED}--- RECARGA RECHAZADA en CP {cp_id_response}. Vuelva a intentar. ---{Colors.RESET}"
>>>>>>> origin/Dani
                CURRENT_CHARGING_CP = None
                SERVICE_CONCLUSION_EVENT.set()
            
            elif decision == 'TICKET':
<<<<<<< HEAD
                UI_STATUS = f"{YELLOW}En espera{RESET}"
                UI_LAST_NOTIFICATION = f"{GREEN}Recarga finalizada en CP {cp_id_response}. Ticket recibido.{RESET}"
=======
                last_notification = f"{Colors.GREEN}--- RECARGA FINALIZADA en CP {cp_id_response}. TICKET RECIBIDO. ---{Colors.RESET}"
>>>>>>> origin/Dani
                CURRENT_CHARGING_CP = None
                SERVICE_CONCLUSION_EVENT.set()

<<<<<<< HEAD
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
=======
        # 2. Comprobar si está ocupado (Modo Fichero o Cargando)
        is_busy = (FILE_PROCESSOR_THREAD and FILE_PROCESSOR_THREAD.is_alive()) or CURRENT_CHARGING_CP is not None
        
        _clear_screen()
        print(f"{Colors.CYAN}{Colors.BOLD}================================================{Colors.RESET}")
        print(f"{Colors.CYAN}{Colors.BOLD}  EV DRIVER TERMINAL (Driver ID: {driver_id}) {Colors.RESET}")
        print(f"{Colors.CYAN}{Colors.BOLD}================================================{Colors.RESET}")

        print(f"{Colors.BOLD} ESTADO ACTUAL: {Colors.RESET}", end="")
        if is_busy:
            if FILE_PROCESSOR_THREAD and FILE_PROCESSOR_THREAD.is_alive():
                print(f"{Colors.MAGENTA}MODO AUTOMÁTICO (Archivo) ACTIVO...{Colors.RESET}")
            elif CURRENT_CHARGING_CP is not None:
                print(f"{Colors.GREEN}CARGANDO en CP {CURRENT_CHARGING_CP}... (Escuchando telemetría){Colors.RESET}")
        else:
            print(f"{Colors.YELLOW}ESPERANDO INSTRUCCIONES...{Colors.RESET}")
        
        print("------------------------------------------------")
        print(f"{Colors.BOLD} ÚLTIMO TICKET: {Colors.RESET}", end="")
        if LAST_TICKET_INFO: print(LAST_TICKET_INFO)
        else: print(f"{Colors.YELLOW}N/A{Colors.RESET}")
        print("------------------------------------------------")

        if last_notification:
            print(f"{last_notification}\n")
            last_notification = ""

        # 3. Mostrar menú o esperar
        if is_busy:
            if not CURRENT_CHARGING_CP:
                 print(f"{Colors.YELLOW}Procesando solicitud... por favor espere.{Colors.RESET}")
            # Si está cargando, la telemetría se imprime sola
            time.sleep(1) 
            continue
        
        print(f"{Colors.BOLD}Selecciona una opcion:{Colors.RESET}")
        print(f" {Colors.YELLOW}[1]{Colors.RESET} Solicitar suministro de CP (Manual)")
        print(f" {Colors.YELLOW}[2]{Colors.RESET} Leer fichero de servicios (Auto)")
        print(f" {Colors.YELLOW}[3]{Colors.RESET} Solicitar el listado de CPs Activos")
        print(f" {Colors.YELLOW}[q]{Colors.RESET} Salir")
        
        opcion = input(f"\n{Colors.BOLD}> {Colors.RESET}").strip().lower()

        if opcion == '1':
            try:
                cp_id_input = str(input(f" {Colors.CYAN}ID del Charging Point: {Colors.RESET}")).upper()
                if not cp_id_input:
                    last_notification = f"{Colors.RED}ID no puede estar vacío.{Colors.RESET}"
                    continue
            except Exception:
                last_notification = f"{Colors.RED}ID de Charging Point invalido.{Colors.RESET}"
                continue

            last_notification = f"{Colors.YELLOW}Enviando solicitud para CP {cp_id_input}...{Colors.RESET}"
            request_service(driver_id, cp_id_input)
        
        elif opcion == '2':
            file_name = input(f" {Colors.CYAN}Nombre del archivo de servicios (ej: servicios.txt): {Colors.RESET}")
            FILE_PROCESSOR_THREAD = threading.Thread(target=process_file_requests, args=(file_name, driver_id), daemon=True)
            FILE_PROCESSOR_THREAD.start()
            last_notification = f"{Colors.MAGENTA}Iniciando modo automático desde '{file_name}'...{Colors.RESET}"
        
        elif opcion == '3':
            show_cp_available(driver_id)
            print(f"{Colors.YELLOW}Esperando respuesta de CPs... (Presione Enter si no aparece nada){Colors.RESET}")
            try:
                input() # Pausa para ver la lista que imprimirá el consumer
            except KeyboardInterrupt:
                break
        
        elif opcion == 'q':
            print(f"{Colors.YELLOW}Saliendo...{Colors.RESET}")
            break
            
        else:
            last_notification = f"{Colors.RED}Opcion invalida.{Colors.RESET}"
>>>>>>> origin/Dani
            
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
    """Punto de entrada principal."""
    global KAFKA_BROKER_ADDR 
    
    if len(sys.argv) != 4:
        print(f"{Colors.RED}Usage: python EV_Driver.py <BROKER_IP> <BROKER_PORT> <DRIVER_ID>{Colors.RESET}")
        return
        
    KAFKA_BROKER_ADDR = [f'{sys.argv[1]}:{sys.argv[2]}']
    DRIVER_ID = sys.argv[3]

    # Inicializar productor global
    if not _initialize_producer():
        print(f"{Colors.RED}No se pudo conectar con Kafka. Saliendo.{Colors.RESET}")
        return

    # Iniciar hilo consumidor
    consumer_thread = threading.Thread(target=receive_responses, args=(DRIVER_ID,), daemon=True)
    consumer_thread.start()
    
    print(f"{Colors.GREEN}[INIT] Esperando 2 segundos a que el consumidor conecte...{Colors.RESET}")
    time.sleep(2) 

    try:
        main_menu_loop(DRIVER_ID)
    except KeyboardInterrupt:
<<<<<<< HEAD
        clear_screen()
        print("\nSaliendo...")
    except Exception as e:
        clear_screen()
        print(f"\n{RED}ERROR INESPERADO EN EL BUCLE PRINCIPAL:{RESET} {e}")
        print("Por favor, reinicie la aplicacion.")
=======
        print(f"\n{Colors.YELLOW}Saliendo...{Colors.RESET}")
    finally:
        # Limpiar productor global al salir
        global KAFKA_PRODUCER
        if KAFKA_PRODUCER:
            print("\nCerrando productor Kafka...")
            KAFKA_PRODUCER.close()
            print("Productor Kafka cerrado.")
>>>>>>> origin/Dani

if __name__ == "__main__":
    main()