# -*- coding: utf-8 -*-
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
import sys
import threading
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
# Estado de la respuesta de Central
RESPONSE_STATE: Dict[str, Optional[Any]] = {"decision": None, "cp_id": None}
RESPONSE_LOCK = threading.Lock() # Protege RESPONSE_STATE
SERVICE_CONCLUSION_EVENT = threading.Event() # Sincroniza el modo fichero

CURRENT_CHARGING_CP: Optional[str] = None # CP al que estamos conectados
LAST_TICKET_INFO: Optional[str] = None
FILE_PROCESSOR_THREAD: Optional[threading.Thread] = None

# Productor de Kafka global y persistente
KAFKA_PRODUCER: Optional[KafkaProducer] = None
PRODUCER_LOCK = threading.Lock()
KAFKA_BROKER_ADDR: List[str] = [] 

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
        KAFKA_PRODUCER = producer
        print(f"{Colors.GREEN}[KAFKA PRODUCER] Productor global inicializado.{Colors.RESET}")
        return True
    except Exception as e:
        print(f"{Colors.RED}[KAFKA PRODUCER] ERROR: Fallo al crear productor global: {e}{Colors.RESET}")
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
    consumer = None
    consumer_group = f'ev_driver_group_{driver_id}'
    global LAST_TICKET_INFO, KAFKA_BROKER_ADDR
    
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
                print(f"{Colors.BLUE}[KAFKA RECV] Recibido en {message.topic}: {response_str}{Colors.RESET}") 
                
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


        except Exception as e:
            print(f"{Colors.RED}[KAFKA CONSUMER] ERROR: Consumer thread failed: {e}. Reintentando en 5s...{Colors.RESET}")
            if consumer is not None:
                consumer.close()
            time.sleep(5) 

# --- 5. FILE PROCESSING LOGIC ---

def process_file_requests(file_name: str, driver_id: str):
    """Procesa una lista de CPs desde un fichero, secuencialmente."""
    try:
        with open(file_name, 'r') as file:
            cp_ids = [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
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
                CURRENT_CHARGING_CP = None
                SERVICE_CONCLUSION_EVENT.set()
                time.sleep(1)
            
            elif decision == 'TICKET':
                last_notification = f"{Colors.GREEN}--- RECARGA FINALIZADA en CP {cp_id_response}. TICKET RECIBIDO. ---{Colors.RESET}"
                CURRENT_CHARGING_CP = None
                SERVICE_CONCLUSION_EVENT.set()
                time.sleep(1)

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
            
        time.sleep(0.1)

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
        print(f"\n{Colors.YELLOW}Saliendo...{Colors.RESET}")
    finally:
        # Limpiar productor global al salir
        global KAFKA_PRODUCER
        if KAFKA_PRODUCER:
            print("\nCerrando productor Kafka...")
            KAFKA_PRODUCER.close()
            print("Productor Kafka cerrado.")

if __name__ == "__main__":
    main()