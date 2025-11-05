# -*- coding: utf-8 -*-
# EV_Driver: Driver application logic for the EVCharging system.
# Implements Request/Reply via Kafka and file-based automatic service requests (Punto 3).
#
# --- MODIFICACIÓN VISUAL ---
# Se han añadido colores ANSI y limpieza de pantalla para una interfaz
# más agradable, sin alterar la lógica de negocio.
# ---------------------------

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
import sys
import threading
import os  # <--- NUEVO: Para limpiar la pantalla
from typing import List, Dict, Any, Optional

# --- 0. VISUALS (NUEVO) ---
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
RESPONSE_STATE: Dict[str, Optional[Any]] = {"decision": None, "cp_id": None}
RESPONSE_LOCK = threading.Lock()
SERVICE_CONCLUSION_EVENT = threading.Event()
CURRENT_CHARGING_CP: Optional[str] = None # NEW: Track the CP the driver is connected to
LAST_TICKET_INFO: Optional[str] = None # NUEVO: Para mostrar el último ticket en la UI

# NUEVO: Estado para el hilo de procesamiento de archivos
FILE_PROCESSOR_THREAD: Optional[threading.Thread] = None

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
        print(f"{Colors.RED}[KAFKA PRODUCER] ERROR: Failed to create producer: {e}{Colors.RESET}")
        return None

def _send_message_sync(producer: KafkaProducer, topic: str, message: str, timeout: int) -> bool:
    """Sends a message synchronously and waits for acknowledgment."""
    try:
        future = producer.send(topic, value=message.encode('utf-8'))
        future.get(timeout=timeout)
        # No imprimir aquí, la UI lo gestiona
        # print(f"[KAFKA] Sent: {message}")
        return True
    except Exception as e:
        print(f"{Colors.RED}[KAFKA PRODUCER] ERROR: An unexpected error occurred during send: {e}{Colors.RESET}")
        return False
    finally:
        producer.close()

def request_service(driver_id: str, cp_id: str, kafka_broker: List[str]):
    """Sends a CP service request message (REQUEST:DRIVER_ID:CP_ID)."""
    global CURRENT_CHARGING_CP
    CURRENT_CHARGING_CP = cp_id # Set the requested CP
    producer = _create_producer(kafka_broker)
    if producer:
        message = f"REQUEST:{driver_id}:{cp_id}"
        _send_message_sync(producer, KAFKA_REQUEST, message, DEFAULT_GET_TIMEOUT)

def send_stop_signal(driver_id: str, cp_id: str, kafka_broker: List[str]):
    """Sends the STOP signal to Central after charging simulation (Punto 9)."""
    producer = _create_producer(kafka_broker)
    if producer:
        message = f"STOP:{driver_id}:{cp_id}" 
        _send_message_sync(producer, KAFKA_REQUEST, message, DEFAULT_GET_TIMEOUT)

# --- 4. SESSION AND CONSUMER LOGIC ---

def handle_charging_session(driver_id: str, cp_id: str, kafka_broker: List[str]):
    """Simulates the charging session and waits for user input to stop (Punto 7, 9)."""
    print(f"\n{Colors.GREEN}<<< INICIANDO RECARGA EN CP {cp_id} (Simulacion: Enchufado) >>>{Colors.RESET}")
    print("El vehiculo esta cargando... Presione [ENTER] para desenchufar y parar.")
    
    # Bloqueo hasta que el usuario presiona Enter (simulando el desenchufado - Punto 187)
    input("") 
    
    # Send the stop signal to Central (Punto 9)
    send_stop_signal(driver_id, cp_id, kafka_broker)
    print(f"{Colors.YELLOW}Senyal de parada enviada. Esperando TICKET final de Central...{Colors.RESET}")
    
    # NOTE: The driver must wait for the TICKET_ENVIADO message from Central to confirm conclusion

def receive_responses(kafka_broker: List[str], driver_id: str):
    """Consumer thread: listens for responses (KAFKA_RESPONSE) and Telemetry (KAFKA_TELEMETRY)."""
    consumer = None
    consumer_group = f'ev_driver_group_{driver_id}'
    global LAST_TICKET_INFO # NUEVO: Acceso a la variable global
    
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
        print(f"{Colors.GREEN}[KAFKA CONSUMER] Successfully connected to broker and listening.{Colors.RESET}")
        
        for message in consumer:
            response_str = message.value.decode('utf-8')
            
            if message.topic == KAFKA_RESPONSE:
                # Format: DECISION:DRIVER_ID:CP_ID (ej: ACEPTADO:2:1)
                parts = response_str.split(':')
                if len(parts) >= 3 and parts[1] == driver_id: # Filter by Driver ID (Punto 194)
                    decision = parts[0]
                    cp_id_response = parts[2]

                    with RESPONSE_LOCK:
                        RESPONSE_STATE["decision"] = decision
                        RESPONSE_STATE["cp_id"] = cp_id_response
                    
                    # (La UI principal se encargará de mostrar la notificación)

                    # Handle final conclusion for file processing thread
                    if decision == 'RECHAZADO' or decision == 'TICKET': # TICKET_ENVIADO is the final conclusion (Punto 9)
                        SERVICE_CONCLUSION_EVENT.set()
                        global CURRENT_CHARGING_CP
                        CURRENT_CHARGING_CP = None
                elif parts[0] == "TICKET" and len(parts) >= 5 and parts[2] == driver_id:  
                    cp_id_ticket = parts[1]  
                    consumo = parts[3]  
                    importe = parts[4]
                    if len(parts)==6 and parts[5]=="AVERIA":
                          print(f"\n{Colors.RED}{Colors.BOLD}<<< TICKET POR AVERIA (CP {cp_id_ticket}) >>>{Colors.RESET}")
                          print(f"{Colors.RED}  [ERROR] El punto de carga ha sufrido una averia")
                          print(f"  Consumo: {consumo} Kwh | Importe: {importe} ${Colors.RESET}")
                          # --- NUEVO: Guardar estado del ticket ---
                          LAST_TICKET_INFO = f"{Colors.RED}AVERIA (CP {cp_id_ticket}): {consumo} Kwh | {importe} ${Colors.RESET}"
                    else:
                        print(f"\n{Colors.GREEN}{Colors.BOLD}<<< TICKET RECIBIDO (CP {cp_id_ticket}) >>>{Colors.RESET}")
                        print(f"{Colors.GREEN}  Consumo: {consumo} Kwh | Importe: {importe} ${Colors.RESET}")
                        # --- NUEVO: Guardar estado del ticket ---
                        LAST_TICKET_INFO = f"{Colors.GREEN}CP {cp_id_ticket}: {consumo} Kwh | {importe} ${Colors.RESET}"
                    
                    # IMPORTANTE: Actualizar el estado para que main_menu_loop lo procese  
                    with RESPONSE_LOCK:  
                        RESPONSE_STATE["decision"] = 'TICKET'  
                        RESPONSE_STATE["cp_id"] = cp_id_ticket  
                    
                    SERVICE_CONCLUSION_EVENT.set()  
                    CURRENT_CHARGING_CP = None
                
                elif parts[0] == "CP_LIST":
                    # --- NUEVO: Formato mejorado para CP_LIST ---
                    print(f"\n{Colors.CYAN}{Colors.BOLD}<<< PUNTOS DE CARGA ACTIVOS >>>{Colors.RESET}")
                    print(f"{Colors.BOLD}  {'CP ID':<8} | {'UBICACION':<25} | {'PRECIO':<10}{Colors.RESET}")
                    print(f"  {'-'*8} | {'-'*25} | {'-'*10}")
                    
                    cp_data_str = response_str[8:]
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
                # Format: CP_ID:CONSUMO:IMPORTE
                tele_parts = response_str.split(':')
                if len(tele_parts) >= 3 and tele_parts[0] == CURRENT_CHARGING_CP: # Filter by the current charging CP (Punto 194)
                    cp_id, consumo, importe = tele_parts[0], tele_parts[1], tele_parts[2]
                    # Punto 8: Mostrar estados del CP
                    # (Se muestra en el bucle principal)
                    # print(f"\n[TELEMETRY CP {cp_id}] Consumo en tiempo real: {consumo}Kw | Importe acumulado: {importe}$")
                    # En lugar de imprimir, actualizamos la UI en el bucle principal si es necesario.
                    # Por ahora, un print está bien si la UI principal está bloqueada.
                    print(f"\r{Colors.CYAN}[TELEMETRY CP {cp_id}] Consumo: {consumo} Kw | Importe: {importe} ${Colors.RESET}   ", end="")


    except Exception as e:
        print(f"{Colors.RED}[KAFKA CONSUMER] ERROR: Consumer thread failed: {e}{Colors.RESET}")
    finally:
        if consumer is not None:
            consumer.close()

# --- 5. FILE PROCESSING LOGIC (Punto 3, 195) ---

def process_file_requests(file_name: str, driver_id: str, kafka_broker: List[str]):
    """Reads CP IDs from file and sends requests sequentially, waiting for conclusion."""
    try:
        with open(file_name, 'r') as file:
            cp_ids = [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print(f"{Colors.RED}[FILE PROCESSOR] ERROR: File '{file_name}' not found.{Colors.RESET}")
        return

    print(f"{Colors.MAGENTA}[FILE PROCESSOR] Starting sequence of {len(cp_ids)} requests.{Colors.RESET}")
    
    for i, cp_id in enumerate(cp_ids):
        print(f"\n{Colors.MAGENTA}[FILE PROCESSOR] --- Starting request {i+1}/{len(cp_ids)} for CP ID: {cp_id} ---{Colors.RESET}")
        
        # 1. Reset the event flag
        SERVICE_CONCLUSION_EVENT.clear()
        
        # 2. Send the request (REQUEST:DRIVER_ID:CP_ID)
        request_service(driver_id, cp_id, kafka_broker)
        
        print(f"{Colors.MAGENTA}[FILE PROCESSOR] Waiting for service conclusion (ACEPTADO, RECHAZADO, or TICKET)...{Colors.RESET}")
        
        # 3. Wait for the service conclusion event
        SERVICE_CONCLUSION_EVENT.wait()  

        print(f"{Colors.MAGENTA}[FILE PROCESSOR] Service concluded. Pausing for {PAUSE_AFTER_SERVICE} seconds (Punto 195)...{Colors.RESET}")
        
        # 4. Wait 4 seconds before next service (Punto 195)
        time.sleep(PAUSE_AFTER_SERVICE)

    print(f"{Colors.MAGENTA}[FILE PROCESSOR] Sequence finished. Returning to manual mode.{Colors.RESET}")
    # Clean up global thread reference
    global FILE_PROCESSOR_THREAD
    FILE_PROCESSOR_THREAD = None

def show_cp_available(driver_id: str, kafka_broker: List[str]):
    print(f"{Colors.YELLOW}[KAFKA] Solicitando lista de CPs activos a Central...{Colors.RESET}")
    producer = _create_producer(kafka_broker)
    if producer:
        message = f"CP_REQUEST:{driver_id}"
        _send_message_sync(producer, KAFKA_REQUEST, message, DEFAULT_GET_TIMEOUT)
    

# --- 6. MAIN APPLICATION LOGIC ---
def main_menu_loop(driver_id: str, kafka_broker: List[str]):
    """Main application loop to display the menu and handle user input."""
    global FILE_PROCESSOR_THREAD
    global CURRENT_CHARGING_CP # Usar la variable global
    global LAST_TICKET_INFO # NUEVO: Acceso para mostrar el ticket
    
    last_notification = "" # NUEVO: Para mostrar mensajes temporales

    while True:
        
        # 1. Check for Kafka Response Status
        with RESPONSE_LOCK:
            decision = RESPONSE_STATE["decision"]
            cp_id_response = RESPONSE_STATE["cp_id"]
            RESPONSE_STATE["decision"] = None # Reset decision status
            
            if decision == 'ACEPTADO':
                # handle_charging_session(driver_id, cp_id_response, kafka_broker)
                last_notification = f"{Colors.GREEN}RECARGA ACEPTADA en CP {cp_id_response}. Conectado.{Colors.RESET}"
                
            elif decision == 'RECHAZADO':
                last_notification = f"{Colors.RED}--- RECARGA RECHAZADA en CP {cp_id_response}. Vuelva a intentar. ---{Colors.RESET}"
                CURRENT_CHARGING_CP = None
                SERVICE_CONCLUSION_EVENT.set()
                time.sleep(1)
            
            elif decision == 'TICKET':
                last_notification = f"{Colors.GREEN}--- RECARGA FINALIZADA en CP {cp_id_response}. TICKET RECIBIDO. ---{Colors.RESET}"
                CURRENT_CHARGING_CP = None
                SERVICE_CONCLUSION_EVENT.set()
                time.sleep(1)

        # 2. Pause the menu if the file processing thread is active or charging session is active (Punto 177)
        is_busy = (FILE_PROCESSOR_THREAD and FILE_PROCESSOR_THREAD.is_alive()) or CURRENT_CHARGING_CP is not None
        
        # --- NUEVO: Redibujar la UI ---
        _clear_screen()
        print(f"{Colors.CYAN}{Colors.BOLD}================================================{Colors.RESET}")
        print(f"{Colors.CYAN}{Colors.BOLD}  EV DRIVER TERMINAL (Driver ID: {driver_id}) {Colors.RESET}")
        print(f"{Colors.CYAN}{Colors.BOLD}================================================{Colors.RESET}")

        # --- NUEVO: Panel de Estado ---
        print(f"{Colors.BOLD} ESTADO ACTUAL: {Colors.RESET}", end="")
        if is_busy:
            if FILE_PROCESSOR_THREAD and FILE_PROCESSOR_THREAD.is_alive():
                print(f"{Colors.MAGENTA}MODO AUTOMÁTICO (Archivo) ACTIVO...{Colors.RESET}")
            elif CURRENT_CHARGING_CP is not None:
                print(f"{Colors.GREEN}CARGANDO en CP {CURRENT_CHARGING_CP}... (Escuchando telemetría){Colors.RESET}")
                # [!!!!!] LÍNEA ELIMINADA (Solicitud del usuario)
        else:
            print(f"{Colors.YELLOW}ESPERANDO INSTRUCCIONES...{Colors.RESET}")
        
        print("------------------------------------------------")

        # --- NUEVO: Panel de Último Ticket ---
        print(f"{Colors.BOLD} ÚLTIMO TICKET: {Colors.RESET}", end="")
        if LAST_TICKET_INFO:
            print(LAST_TICKET_INFO)
        else:
            print(f"{Colors.YELLOW}N/A{Colors.RESET}")

        print("------------------------------------------------")


        # Mostrar la última notificación si existe
        if last_notification:
            print(f"{last_notification}\n")
            last_notification = "" # Limpiar después de mostrar

        # 3. Display Menu (Solo si no está ocupado)
        if is_busy:
            print(f"{Colors.YELLOW}Procesando solicitud... por favor espere.{Colors.RESET}")
            time.sleep(1) 
            continue # Go back to the top of the loop to check for Kafka messages/status
        
        print(f"{Colors.BOLD}Selecciona una opcion:{Colors.RESET}")
        print(f" {Colors.YELLOW}[1]{Colors.RESET} Solicitar suministro de CP (Manual)")
        print(f" {Colors.YELLOW}[2]{Colors.RESET} Leer fichero de servicios (Auto)")
        print(f" {Colors.YELLOW}[3]{Colors.RESET} Solicitar el listado de CPs Activos")
        
        opcion = input(f"\n{Colors.BOLD}> {Colors.RESET}")

        if opcion == '1':
            try:
                cp_id_input = str(input(f" {Colors.CYAN}Seleccione un Charging Point para solicitar suministro: {Colors.RESET}"))
            except Exception:
                last_notification = f"{Colors.RED}ID de Charging Point invalido.{Colors.RESET}"
                continue

            last_notification = f"{Colors.YELLOW}Enviando solicitud para CP {cp_id_input}...{Colors.RESET}"
            request_service(driver_id, cp_id_input, kafka_broker)
        
        elif opcion == '2':
            file_name = input(f" {Colors.CYAN}Ingrese el nombre del archivo de servicios (ej: servicios.txt): {Colors.RESET}")
            # Start file processing and save the thread reference
            FILE_PROCESSOR_THREAD = threading.Thread(target=process_file_requests, args=(file_name, driver_id, kafka_broker), daemon=True)
            FILE_PROCESSOR_THREAD.start()
            last_notification = f"{Colors.MAGENTA}Iniciando modo automático desde '{file_name}'...{Colors.RESET}"
        
        elif opcion == '3': # <-- ACCIÓN PARA LA OPCIÓN 3
            show_cp_available(driver_id, kafka_broker)
            # Pausa para que el usuario pueda leer la lista que imprimirá el consumer
            print(f"{Colors.YELLOW}Esperando respuesta de CPs... (Presione Enter si no aparece nada){Colors.RESET}")
            input() # Pausa
        
        else:
            last_notification = f"{Colors.RED}Opcion invalida.{Colors.RESET}"
            
        time.sleep(0.1)

def main():
    """Main execution entry point."""
    if len(sys.argv) != 4:
        print(f"{Colors.RED}Usage: python EV_Driver.py <BROKER_IP> <BROKER_PORT> <DRIVER_ID>{Colors.RESET}")
        return
        
    KAFKA_BROKER = [f'{sys.argv[1]}:{sys.argv[2]}']
    DRIVER_ID = sys.argv[3]

    # Start the response listener thread (must be started first)
    consumer_thread = threading.Thread(target=receive_responses, args=(KAFKA_BROKER, DRIVER_ID), daemon=True)
    consumer_thread.start()
    
    # Espera un segundo para que el consumidor se conecte antes de limpiar
    time.sleep(1) 

    try:
        main_menu_loop(DRIVER_ID, KAFKA_BROKER)
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Exiting...{Colors.RESET}")

if __name__ == "__main__":
    main()