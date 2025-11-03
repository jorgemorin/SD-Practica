# -*- coding: utf-8 -*-
# EV_Driver: Driver application logic for the EVCharging system.
# Implements Request/Reply via Kafka and file-based automatic service requests (Punto 3).

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
import sys
import threading
from typing import List, Dict, Any, Optional

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
        print(f"[KAFKA PRODUCER] ERROR: Failed to create producer: {e}")
        return None

def _send_message_sync(producer: KafkaProducer, topic: str, message: str, timeout: int) -> bool:
    """Sends a message synchronously and waits for acknowledgment."""
    try:
        future = producer.send(topic, value=message.encode('utf-8'))
        future.get(timeout=timeout)
        print(f"[KAFKA] Sent: {message}")
        return True
    except Exception as e:
        print(f"[KAFKA PRODUCER] ERROR: An unexpected error occurred during send: {e}")
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
    print(f"\n<<< INICIANDO RECARGA EN CP {cp_id} (Simulacion: Enchufado) >>>")
    print("El vehiculo esta cargando... Presione [ENTER] para desenchufar y parar.")
    
    # Bloqueo hasta que el usuario presiona Enter (simulando el desenchufado - Punto 187)
    input("") 
    
    # Send the stop signal to Central (Punto 9)
    send_stop_signal(driver_id, cp_id, kafka_broker)
    print("Senyal de parada enviada. Esperando TICKET final de Central...")
    
    # NOTE: The driver must wait for the TICKET_ENVIADO message from Central to confirm conclusion

def receive_responses(kafka_broker: List[str], driver_id: str):
    """Consumer thread: listens for responses (KAFKA_RESPONSE) and Telemetry (KAFKA_TELEMETRY)."""
    consumer = None
    consumer_group = f'ev_driver_group_{driver_id}'
    
    # Driver only receives messages of the CP providing the service (Punto 194).
    # Since Kafka is a broadcast system, the consumer must filter messages.

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
        print("[KAFKA CONSUMER] Successfully connected to broker and listening.")
        
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
                        
                    print(f"\n<<< RECEIVED NOTIFICATION >>> Decision for CP {cp_id_response}: {decision}")

                    # Handle final conclusion for file processing thread
                    if decision == 'RECHAZADO' or decision == 'TICKET_ENVIADO': # TICKET_ENVIADO is the final conclusion (Punto 9)
                        SERVICE_CONCLUSION_EVENT.set()
                        global CURRENT_CHARGING_CP
                        CURRENT_CHARGING_CP = None

            elif message.topic == KAFKA_TELEMETRY:
                # Format: CP_ID:CONSUMO:IMPORTE
                tele_parts = response_str.split(':')
                if len(tele_parts) >= 3 and tele_parts[0] == CURRENT_CHARGING_CP: # Filter by the current charging CP (Punto 194)
                    cp_id, consumo, importe = tele_parts[0], tele_parts[1], tele_parts[2]
                    # Punto 8: Mostrar estados del CP
                    print(f"\n[TELEMETRY CP {cp_id}] Consumo en tiempo real: {consumo}Kw | Importe acumulado: {importe}$")

    except Exception as e:
        print(f"[KAFKA CONSUMER] ERROR: Consumer thread failed: {e}")
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
        print(f"[FILE PROCESSOR] ERROR: File '{file_name}' not found.")
        return

    print(f"[FILE PROCESSOR] Starting sequence of {len(cp_ids)} requests.")
    
    for i, cp_id in enumerate(cp_ids):
        print(f"\n[FILE PROCESSOR] --- Starting request {i+1}/{len(cp_ids)} for CP ID: {cp_id} ---")
        
        # 1. Reset the event flag
        SERVICE_CONCLUSION_EVENT.clear()
        
        # 2. Send the request (REQUEST:DRIVER_ID:CP_ID)
        request_service(driver_id, cp_id, kafka_broker)
        
        print("[FILE PROCESSOR] Waiting for service conclusion (ACEPTADO, RECHAZADO, or TICKET_ENVIADO)...")
        
        # 3. Wait for the service conclusion event (set by ACEPTADO/RECHAZADO/TICKET_ENVIADO in receive_responses)
        SERVICE_CONCLUSION_EVENT.wait()  # This blocks until Central sends final conclusion/rejection

        # Handle ACEPTADO: If accepted, the thread needs to wait for the user to input stop (simulating the charge).
        # This is handled in main_menu_loop after the ACEPTADO response is processed.
        # The main_menu_loop will call handle_charging_session which will block until user input and then send STOP.
        # The thread will only proceed when TICKET_ENVIADO is received by the consumer, triggering SERVICE_CONCLUSION_EVENT.set()
        
        print(f"[FILE PROCESSOR] Service concluded. Pausing for {PAUSE_AFTER_SERVICE} seconds (Punto 195)...")
        
        # 4. Wait 4 seconds before next service (Punto 195)
        time.sleep(PAUSE_AFTER_SERVICE)

    print("[FILE PROCESSOR] Sequence finished. Returning to manual mode.")
    # Clean up global thread reference
    global FILE_PROCESSOR_THREAD
    FILE_PROCESSOR_THREAD = None

def show_cp_available():
    print("CP DISPONIBLES")

# --- 6. MAIN APPLICATION LOGIC ---
def main_menu_loop(driver_id: str, kafka_broker: List[str]):
    """Main application loop to display the menu and handle user input."""
    global FILE_PROCESSOR_THREAD
    global CURRENT_CHARGING_CP # Usar la variable global

    while True:
        
        # 1. Check for Kafka Response Status and handle charging session
        with RESPONSE_LOCK:
            decision = RESPONSE_STATE["decision"]
            cp_id_response = RESPONSE_STATE["cp_id"]
            RESPONSE_STATE["decision"] = None # Reset decision status
            
            if decision == 'ACEPTADO':
                # Start charging session (blocks until user input)
                handle_charging_session(driver_id, cp_id_response, kafka_broker)
                
                # [!!!!!] QUITAR EL handle_charging_session y poner una interfaz de "cargando",
                # conectado mediante un nuevo topic directamente a Engine, quien va contando
                # cuanto tiempo va recargando y como va aumentando el precio, tiene que salir
                # por pantalla por aquí, solo será necesario un kafka consumer (desde aquí), y
                # un kafka producer desde Engine.
                # Por otra parte, en handle_charging_session, se pondrá en Engine, (con un thread)
                # y se manejará la recarga (pausa manual y demás) desde allá.
                
            elif decision == 'RECHAZADO':
                print(f"--- RECARGA RECHAZADA en CP {cp_id_response}. Vuelva a intentar. ---")
                CURRENT_CHARGING_CP = None
                # If in auto mode, signal conclusion for 4-second pause
                SERVICE_CONCLUSION_EVENT.set()
                time.sleep(1)
            
            elif decision == 'TICKET_ENVIADO':
                print(f"--- RECARGA FINALIZADA en CP {cp_id_response}. TICKET RECIBIDO. ---")
                CURRENT_CHARGING_CP = None
                # If in auto mode, signal conclusion for 4-second pause
                SERVICE_CONCLUSION_EVENT.set()
                time.sleep(1)

        # 2. Pause the menu if the file processing thread is active or charging session is active (Punto 177)
        if (FILE_PROCESSOR_THREAD and FILE_PROCESSOR_THREAD.is_alive()) or CURRENT_CHARGING_CP is not None:
            time.sleep(1) 
            continue # Go back to the top of the loop to check for Kafka messages/status

        # 3. Display Menu and get user option

        show_cp_available()

        print("\n================================================")
        print(f"DRIVER {driver_id}")
        print("================================================")
        print("Selecciona una opcion:")
        print(" [1] Solicitar suministro de CP (Manual)")
        print(" [2] Leer fichero de servicios (Auto)")
        
        opcion = input("> ")

        if opcion == '1':
            try:
                cp_id_input = str(input("Seleccione un Charging Point para solicitar suministro: "))
            except Exception:
                print("ID de Charging Point invalido.")
                continue

            # Send the service request
            request_service(driver_id, cp_id_input, kafka_broker)
        
        elif opcion == '2':
            file_name = input("Ingrese el nombre del archivo de servicios (ej: servicios.txt): ")
            # Start file processing and save the thread reference
            FILE_PROCESSOR_THREAD = threading.Thread(target=process_file_requests, args=(file_name, driver_id, kafka_broker), daemon=True)
            FILE_PROCESSOR_THREAD.start()
        
        else:
            print("Opcion invalida.")
            
        time.sleep(0.1)

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
    
    time.sleep(2) # Give the consumer thread time to connect

    try:
        main_menu_loop(DRIVER_ID, KAFKA_BROKER)
    except KeyboardInterrupt:
        print("\nExiting...")

if __name__ == "__main__":
    main()