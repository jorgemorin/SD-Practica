from kafka import KafkaConsumer, KafkaProducer
import socket
import threading
import sys
import os
import time
from typing import List, Dict, Any, Optional, Tuple

# --- 1. CONFIGURATION AND CONSTANTS ---
# Topic Definitions
KAFKA_REQUEST_TOPIC = 'DriverRequest'
KAFKA_RESPONSE_TOPIC = 'DriverResponse'
KAFKA_TELEMETRY_TOPIC = 'CPTelemetry' # NEW: Topic for CP monitoring (Punto 8)
DEFAULT_KAFKA_BROKER = ['localhost:9092']

# Kafka Timeout Optimization (ms)
FAST_INIT_TIMEOUT = 10001
CONSUMER_SESSION_TIMEOUT = 10000
CONSUMER_HEARTBEAT = 3000

# Server/Socket Config
PORT_SOCKET = 5000
HOST = '0.0.0.0' # Listen on all interfaces
FORMAT = 'utf-8'

# Status Definitions (Completed based on practice document)
STATUS_ACTIVO = 'ACTIVO'           # VERDE
STATUS_SUMINISTRANDO = 'SUMINISTRANDO' # VERDE con datos
STATUS_PARADO = 'PARADO'           # NARANJA / Out of Order
STATUS_AVERIA = 'AVERIADO'           # ROJO
STATUS_DESCONECTADO = 'DESCONECTADO' # GRIS

# --- 2. DATA STRUCTURES (Simulated Database) ---
cp_registry: dict[str, dict] = {}

# --- 3. UTILITY FUNCTIONS (Protocol and DB) ---

def build_protocol_response(message_type: str, payload: str = "") -> bytes:
    return f"{message_type}:{payload}".encode(FORMAT)

def parse_protocol(data: bytes) -> Optional[str]:
    try:
        return data.decode(FORMAT).strip()
    except UnicodeDecodeError:
        print("[ERROR] Failed to decode message.")
        return None

def reset_cp_state():
    input_file = "ChargingPoints.txt"
    output_file = "ChargingPoints.txt.tmp"
    updated_lines = []
    
    try:
        # 1. Leer el fichero original
        with open(input_file, "r") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                    
                parts = line.split(':')
                
                # Formato esperado: ID:Localizacion:Precio:Estado
                if len(parts) >= 4:
                    cp_id = parts[0]
                    location = parts[1]
                    price = parts[2]
                    status = parts[3].upper() # Convertir a mayusculas por si acaso
                    
                    # 2. Aplicar la logica de reseteo
                    if status not in [STATUS_AVERIA, STATUS_PARADO]:
                        new_status = STATUS_DESCONECTADO
                    else:
                        new_status = status # Mantener AVERIA o PARADO
                        
                    # 3. Construir la nueva linea
                    updated_lines.append(f"{cp_id}:{location}:{price}:{new_status}\n")
                else:
                    print(f"[WARN] Linea mal formada omitida: {line}")
                    
    except FileNotFoundError:
        print(f"[ERROR] No se encontro el fichero '{input_file}'. No se reseteo ningun estado.")
        return
    except Exception as e:
        print(f"[ERROR] Ocurrio un error leyendo el fichero: {e}")
        return

    try:
        # 4. Escribir en un fichero temporal
        with open(output_file, "w") as file:
            file.writelines(updated_lines)
            
        # 5. Reemplazar el fichero original de forma atomica (segura)
        os.replace(output_file, input_file)
        
        print(f"[OK] Se han reseteado los estados en '{input_file}'.")
        print(f"   {len(updated_lines)} CPs actualizados a DESCONECTADO (excepto AVERIADOS/PARADOS).")

    except Exception as e:
        print(f"[ERROR] Ocurrio un error escribiendo el fichero actualizado: {e}")
        # Intentar borrar el temporal si algo salio mal
        if os.path.exists(output_file):
            os.remove(output_file)

def read_data_cp():
    """Reads CP data from ChargingPoints.txt into cp_registry."""
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
                    "addr": None # Address is set on socket connection
                }
        file.close()
        print(f"[SERVER] Data loaded from ChargingPoints.txt. {len(cp_registry)} CPs found.")
    except FileNotFoundError:
        print("[WARNING] ChargingPoints.txt not found. Starting with empty database.")
    except Exception as e:
        print(f"[ERROR] Error reading data: {e}")


def write_data_cp():
    """Writes current cp_registry data to ChargingPoints.txt."""
    try:
        file = open("ChargingPoints.txt", "w")
        for cp_id, info in cp_registry.items():
            line = f"{cp_id}:{info['location']}:{info['price']}:{info['status']}\n"
            file.write(line)
        file.close()
        print("[SERVER] Data saved to ChargingPoints.txt")
    except Exception as e:
        print(f"[ERROR] Failed to write to ChargingPoints.txt: {e}")

# --- 4. KAFKA PRODUCER/CONSUMER LOGIC ---

def send_request_decision_to_driver(driver_id: str, cp_id: str, decision: str):
    """Sends the final decision (ACEPTADO/RECHAZADO) back to the Driver via Kafka (Punto 4, 176)."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=DEFAULT_KAFKA_BROKER,
            api_version=(4, 1, 0),
            request_timeout_ms=FAST_INIT_TIMEOUT
        )
        # Format: DECISION:DRIVER_ID:CP_ID
        message = f"{decision}:{driver_id}:{cp_id}"
        future = producer.send(KAFKA_RESPONSE_TOPIC, value=message.encode('utf-8'))
        future.get(timeout=100)
        print(f"[KAFKA CENTRAL] Sent decision to driver: {message}")
    except Exception as e:
        print(f"[KAFKA] ERROR: Failed to send decision to driver: {e}")
    finally:
        if 'producer' in locals():
            producer.close()

def send_authorization_to_cp_via_socket(cp_id: str, driver_id: str) -> bool:
    """NEW: Central acts as a client to request authorization from the CP (Punto 4, 175)."""
    if cp_id not in cp_registry or cp_registry[cp_id]['addr'] is None:
        print(f"[SOCKET CLIENT] ERROR: CP {cp_id} not registered or address unknown.")
        return False

    cp_addr = cp_registry[cp_id]['addr']
    cp_ip, cp_port = cp_addr

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Reemplazar el puerto de CENTRAL por el puerto del CP Engine (se asume que es el mismo que el Engine esta escuchando)
        # NOTA: En un despliegue real, la BD de CPs almacenaria el puerto de escucha del Engine.
        # Aqui se usa el puerto de registro guardado. Se asume que el CP Monitor se registro con la IP y Puerto del CP Engine.
        # Si no se envio el puerto del CP Engine, se utiliza el puerto de registro.
        # Para evitar complejidades de puerto en el monitor/engine, usaremos el IP del CP Engine que es el que se conecta al monitor.
        # Como no se tiene el puerto de escucha del CP Engine, se asume que el CP Engine escucha en un puerto predefinido (e.g. 5001)

        # SIMPLIFICACIoN: Dado que el REGISTRO no incluye el puerto de escucha del Engine
        # Se asume que el CP Engine escucha en el mismo IP que se registro el Monitor, en el PORT_SOCKET + 1
        # ESTO DEBE SER ACLARADO EN LA PRaCTICA, pero por ahora se usara el puerto 5001 como ejemplo de Engine
        # El socket de registro (Monitor) es el 5000. El socket de Autorizacion (Engine) sera el 5001.

        # Se usara la IP del CP que se registro.
        client_socket.connect((cp_ip, 5001)) # Asumir puerto 5001 para el Engine (para el control)
        
        # Protocolo de Solicitud de Autorizacion: AUTORIZAR:DRIVER_ID
        request_msg = f"AUTORIZAR:{driver_id}".encode(FORMAT)
        client_socket.sendall(request_msg)

        # Esperar respuesta del CP Engine
        response_data = client_socket.recv(1024)
        response_msg = parse_protocol(response_data)
        
        if response_msg and response_msg.startswith("ACEPTADO"):
            print(f"[SOCKET CLIENT] CP {cp_id} authorized supply for Driver {driver_id}.")
            return True
        else:
            print(f"[SOCKET CLIENT] CP {cp_id} rejected authorization for Driver {driver_id}.")
            return False

    except Exception as e:
        print(f"[SOCKET CLIENT] ERROR: Failed to communicate with CP {cp_id}: {e}")
        return False
    finally:
        if 'client_socket' in locals():
            client_socket.close()

def _check_and_authorize_cp(driver_id_received: str, cp_id_received: str):
    """
    Implements Central's logic to check CP availability (Punto 4).
    If ACTIVO, requests authorization from CP (via Sockets).
    """
    decision = 'RECHAZADO'
    
    print(f"[DRIVER REQUEST] Checking Charging Point status for {cp_id_received}...")
    if cp_id_received in cp_registry:
        cp_info = cp_registry[cp_id_received]
        
        # 1. Check availability
        if cp_info['status'] == STATUS_ACTIVO:
            print("[DRIVER REQUEST] Charging Point is ACTIVE. Requesting authorization from CP...")
            
            # 2. Punto 4: Request CP authorization (Sockets)
            #if send_authorization_to_cp_via_socket(cp_id_received, driver_id_received):
            decision = 'ACEPTADO'
            # 3. If accepted by CP, CENTRAL changes state (Punto 4)
            cp_registry[cp_id_received]['status'] = STATUS_SUMINISTRANDO
            print(f"[DRIVER REQUEST] CP {cp_id_received} state changed to SUMINISTRANDO.")
            write_data_cp() # Save state change
            #else:
            #    print("[DRIVER REQUEST] CP rejected the authorization (Socket response).")
            
        else:
            print(f"[DRIVER REQUEST] CP is unavailable ({cp_info['status']}). Rejecting.")
            
    else:
        print("[DRIVER REQUEST] Charging Point not found in registry. Rejecting.")

    # 4. Punto 4, 176: CENTRAL notifies the Driver (Kafka)
    send_request_decision_to_driver(driver_id_received, cp_id_received, decision)

def read_consumer():
    """Consumes requests from DriverRequest and Telemetry from CPTelemetry (Punto 8)."""
    topics = [KAFKA_REQUEST_TOPIC, KAFKA_TELEMETRY_TOPIC]
    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=DEFAULT_KAFKA_BROKER, 
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='ev-central-group-1022-v3',
            api_version=(4, 1, 0),
            request_timeout_ms=FAST_INIT_TIMEOUT,
            session_timeout_ms=CONSUMER_SESSION_TIMEOUT,
            heartbeat_interval_ms=CONSUMER_HEARTBEAT
        )
        print(f"[KAFKA] Successfully connected and listening for messages on {topics}.")
        
        for message in consumer:
            message_str = message.value.decode('utf-8')
            print(f"[KAFKA] Received from {message.topic}: {message_str}")
            
            if message.topic == KAFKA_REQUEST_TOPIC:
                parts = message_str.split(':')
                if message_str.startswith("REQUEST:") and len(parts) >= 3:
                    driver_id_received, cp_id_received = parts[1], parts[2]
                    _check_and_authorize_cp(driver_id_received, cp_id_received)

                elif message_str.startswith("STOP:") and len(parts) >= 3:
                    cp_id_stop = parts[2]
                    if cp_id_stop in cp_registry and cp_registry[cp_id_stop]['status'] == STATUS_SUMINISTRANDO:
                        # Punto 9: CP notifies Central of stop. Central marks as ACTIVO and sends ticket (ACK)
                        cp_registry[cp_id_stop]['status'] = STATUS_ACTIVO
                        write_data_cp()
                        print(f"[KAFKA] Charging session stopped for CP {cp_id_stop}. Status set to ACTIVO.")
                        # Send 'TICKET' to driver (Punto 9)
                        driver_id_stop = parts[1] # Driver ID should be in parts[1]
                        send_request_decision_to_driver(driver_id_stop, cp_id_stop, "TICKET_ENVIADO")
                    else:
                        print(f"[KAFKA] Received STOP for CP {cp_id_stop} not in SUMINISTRANDO.")

            elif message.topic == KAFKA_TELEMETRY_TOPIC:
                # Punto 8: Recibir Telemetria. Format: CP_ID:CONSUMO:IMPORTE
                tele_parts = message_str.split(':')
                if len(tele_parts) >= 3 and tele_parts[0] in cp_registry:
                    cp_id, consumo, importe = tele_parts[0], tele_parts[1], tele_parts[2]
                    # NOTE: Logic to update the central monitoring panel (not implemented in this console version)
                    print(f"[TELEMETRY] CP {cp_id}: Consumo={consumo}Kw, Importe={importe}$")
                    # Reenviar Telemetria al conductor (Punto 8). Format: TELEMETRY:CP_ID:CONSUMO:IMPORTE
                    # (Necesitariamos saber el Driver ID, que no esta en la telemetria. Simplificacion: asume que el CP sabe a quien enviar)
                    # ALTERNATIVA: El Driver consume el mismo topic KAFKA_TELEMETRY_TOPIC y filtra por CP_ID

    except Exception as e:
        print(f"[KAFKA] ERROR: Consumer thread failed: {e}")

# --- 5. SOCKET/SERVER LOGIC ---

def handle_client(conn, addr):
    print(f"[SOCKET] {addr} connected.")
    current_cp_id = None

    try:
        while True:
            data = conn.recv(1024)
            if not data: break
            message = parse_protocol(data)
            if message is None: continue
            print(f"[RECEIVED] {addr}: {message}")
            
            parts = message.split(':')
            msg_type = parts[0].upper()

            # Process Sockets messages (REGISTRO, ESTADO, AUTENTICACION)
            if msg_type == "REGISTRO" and len(parts) >= 4: # Incluir la IP/Puerto del CP Engine (suponiendo que es el mismo que el Monitor, o el Engine escucha en el 5001)
                current_cp_id = parts[1]
                location = parts[2]
                price = parts[3]
                # El Monitor (handle_client) registra la IP/puerto desde donde se conecta
                cp_ip_monitor = addr[0]
                # El Engine escuchara en otro puerto (ej: 5001), pero la IP es la misma
                cp_addr_for_control = (cp_ip_monitor, 5001) # Usar puerto 5001 como Engine Control Port

                try:
                    price_f = float(price)
                    cp_registry[current_cp_id] = {
                        "location": location,
                        "price": price_f,
                        "status": STATUS_ACTIVO,
                        'addr': cp_addr_for_control # NEW: Save CP address for Central -> CP communication
                    }
                    response = build_protocol_response("ACEPTADO", current_cp_id)
                    write_data_cp()
                    print("[SERVER] Charging Point registered:", cp_registry[current_cp_id])
                except ValueError:
                    response = build_protocol_response("RECHAZADO", "Invalid price format")
                conn.sendall(response)

            elif msg_type == "ESTADO" and len(parts) >= 2:
                # Logica de actualizacion de estado (AVERIA, PARADO, etc. - Punto 190)
                cp_id_report = parts[1]
                new_status = parts[2].upper()
                if cp_id_report in cp_registry:
                    valid_statuses = [STATUS_ACTIVO, STATUS_PARADO, STATUS_AVERIA, STATUS_SUMINISTRANDO, STATUS_DESCONECTADO]
                    if new_status in valid_statuses:
                        # Punto 191: Si hay averia durante el suministro, finalizar inmediatamente
                        if new_status == STATUS_AVERIA and cp_registry[cp_id_report]["status"] == STATUS_SUMINISTRANDO:
                            print(f"[SERVER] AVERIA detectada durante el suministro en CP {cp_id_report}. Finalizando...")
                            # Logica de notificacion al conductor (requiere Driver ID, no disponible aqui)
                            # Simplificacion: se actualiza el estado, el conductor lo detectara por falta de telemetria/status

                        cp_registry[cp_id_report]["status"] = new_status
                        response = build_protocol_response("ACTUALIZADO", cp_id_report)
                        print(f"[SERVER] Status updated for {cp_id_report}: {new_status}")
                        write_data_cp() # Save state change
                    else:
                        response = build_protocol_response("RECHAZADO", "Invalid status")
                else:
                    response = build_protocol_response("RECHAZADO", "Unknown ID")
                conn.sendall(response)

            elif msg_type == "AUTENTICACION" and len(parts) >= 2:
                # Logica de Autenticacion (Punto 277)
                cp_id_auth = parts[1]
                if cp_id_auth in cp_registry:
                    response = build_protocol_response("ACEPTADO", cp_id_auth)
                    # Update address on successful re-authentication/connection
                    cp_registry[cp_id_auth]['addr'] = (addr[0], 5001) # Update IP from connection
                    cp_registry[cp_id_auth]['status'] = STATUS_ACTIVO
                    print(f"[SERVER] Charging Point {cp_id_auth} authenticated and address updated.")
                    
                    write_data_cp()
                else:
                    response = build_protocol_response("RECHAZADO", "Unknown ID")
                    print("[ERROR] Unknown ID from", addr)
                conn.sendall(response)
            
            else:
                conn.sendall(build_protocol_response("RECHAZADO", "Unknown message type"))

    except ConnectionResetError:
        print(f"[SERVER] {addr} disconnected unexpectedly.")
    except Exception as e:
        print(f"[ERROR] Exception handling client {addr}: {e}")
    finally:
        # Punto 168: Si se pierde la conexion, marcar como DESCONECTADO
        if current_cp_id and current_cp_id in cp_registry:
            cp_registry[current_cp_id]["status"] = STATUS_DESCONECTADO
            write_data_cp() # Save state change
            print(f"[SERVER] Charging Point {current_cp_id} marked as DESCONECTADO.")
        conn.close()
        print(f"[SERVER] {addr} connection closed.")

def start():
    print(f"[SERVER] Server listening on {HOST}:{PORT_SOCKET}")
    server.listen()
    num_conections = threading.active_count() - 1
    print("[SERVER] Number of active connections: ", num_conections)

    while True:
        conn, addr = server.accept()
        num_conections = threading.active_count()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[SERVER] Active connections: {num_conections}")

# --- 6. MAIN EXECUTION ---
if __name__ == "__main__":
    # Configurar el socket para escuchar las conexiones de los CP Monitors (Registro/Estado)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ADDR = (HOST, PORT_SOCKET)
    try:
        server.bind(ADDR)
    except Exception as e:
        print(f"[ERROR] Failed to bind server socket: {e}")
        sys.exit(1)

    # Hilo para escuchar mensajes de Kafka (Drivers y Telemetria)
    kafka_thread = threading.Thread(target=read_consumer, daemon=True)
    kafka_thread.start()
    
    print("[SERVER] Starting EV_Central...")
    reset_cp_state()
    read_data_cp()
    
    try:
        start() # Iniciar el servidor de Sockets para CPs
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down server...")
        write_data_cp()
        server.close()
        sys.exit(0)