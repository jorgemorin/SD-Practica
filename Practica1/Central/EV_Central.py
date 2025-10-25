# -*- coding: utf-8 -*-

# EV_Central

# Sockets:
# - Recepci?n de conexi?n de los Charging Points

# Recibe de los Charging Points:
# - ID del punto de recarga
# - Ubicaci?n del punto de recarga
# - Precio que tiene el suministro
# - Estado (Activado/Parado/Suministrando/Averiado/Desconectado)

from kafka import KafkaConsumer
import socket
import threading
import sys

#MAL-Reenvia el mensaje por el mismo topic, entonces lo vuelve a interpretar el mismo Central, creo que debería crear otro topic y ponerlo a funcionar para las respuestas a los Drivers
def send_request_decision_to_driver(driver_id, cp_id, decision):
	KAFKA_BROKER = ['localhost:9092'] 
	KAFKA_TOPIC = 'EVCharging'
	
	try:
		from kafka import KafkaProducer
		producer = KafkaProducer(
			bootstrap_servers=KAFKA_BROKER,
			api_version=(4, 1, 0),
			request_timeout_ms=10001
		)
		message = f"REQUEST:{driver_id}:{cp_id}:{decision}"
		future = producer.send(KAFKA_TOPIC, value=message.encode('utf-8'))
		future.get(timeout=100)
		print(f"[KAFKA] Sent decision to driver: {message}")
		
	except Exception as e:
		print(f"[KAFKA] ERROR: Failed to send decision to driver: {e}")
		
	finally:
		if 'producer' in locals():
			producer.flush()
			producer.close()

def read_consumer():
	KAFKA_BROKER = ['localhost:9092'] 
	KAFKA_TOPIC = 'EVCharging'
    
	try:
		consumer = KafkaConsumer(
			KAFKA_TOPIC,
			bootstrap_servers=KAFKA_BROKER, 
			auto_offset_reset='latest',
			enable_auto_commit=True,
			group_id='ev-central-group-1022-v3',
			api_version=(4, 1, 0),
            request_timeout_ms=10001
		)
		print("[KAFKA] Successfully connected and listening for messages.")
        
		for message in consumer:
			print(f"[KAFKA] {message.value.decode('utf-8')}")
			if message.value.decode('utf-8').startswith("DRIVER:"):
				# El Driver se conecta (NO SE SI ES NECESARIO COMO TAL)
				continue
			elif message.value.decode('utf-8').startswith("REQUEST:"):
				driver_id_received = message.value.decode('utf-8').split(':')[1]
				cp_id_received = message.value.decode('utf-8').split(':')[2]

				print("[DRIVER REQUEST] Driver ID:", driver_id_received, "Charging Point ID:", cp_id_received)
				print("[DRIVER REQUEST] Checking Charging Point status...")
				if cp_id_received in cp_registry:
					print("[DRIVER REQUEST] Charging Point found in registry.")
					cp_info = cp_registry[cp_id_received]
					print("[DRIVER REQUEST] Charging Point status:", cp_info['status'])
					if cp_info['status'] == 'ACTIVO':
						print("[DRIVER REQUEST] Charging Point is ACTIVE. Accepting request.")
						decision = 'ACEPTADO'
					else:
						print("[DRIVER REQUEST] Charging Point is not ACTIVE. Rejecting request.")
						decision = 'RECHAZADO'
				else:
					print("[DRIVER REQUEST] Charging Point not found in registry. Rejecting request.")
					decision = 'RECHAZADO'

				send_request_decision_to_driver(driver_id_received, cp_id_received, decision)
				
	except Exception as e:
		print(f"[KAFKA] ERROR: Failed to connect to Kafka Broker: {e}")

# ====================================================================== #
# Server configuration and arguments management
# ====================================================================== #
if len(sys.argv) >= 2:
	try:
		PORT_SOCKET = int(sys.argv[1])
	except ValueError:
		print("Invalid port number. Using default port 5000.")
		PORT_SOCKET = 5000
else:
	PORT_SOCKET = 5000

HOST = '127.0.0.1'
FORMAT = 'utf-8'

# ====================================================================== #
# Simulated Database
# ====================================================================== #
cp_registry = {}
driver_registry = {}

def read_data_cp():
	"""================================================"""
	""" Reading data from ChargingPoints.txt		           """
	"""================================================"""
	try:
		file = open("ChargingPoints.txt", "r")
		for line in file:
		# [FORMATO] : ID:UBICACION:PRECIO:ESTADO
			parts = line.strip().split(':')
			if len(parts) >= 4:
				cp_id = parts[0]
				location = parts[1]
				price = float(parts[2])
				status = parts[3]
				cp_registry[cp_id] = {
					"location": location,
					"price": price,
					"status": status
				}
		file.close()
		print("[SERVER] Data loaded from ChargingPoints.txt")
	
	except FileNotFoundError:
		print("[WARNING] ChargingPoints.txt not found. Starting with empty database.")

def write_data_cp():
	"""================================================"""
	""" Writing data to ChargingPoints.txt		           """
	"""================================================"""
	try:
		file = open("ChargingPoints.txt", "w")
		for cp_id, info in cp_registry.items():
			line = f"{cp_id}:{info['location']}:{info['price']}:{info['status']}\n"
			file.write(line)
		file.close()
		print("[SERVER] Data saved to ChargingPoints.txt")
	
	except Exception as e:
		print(f"[ERROR] Failed to write to ChargingPoints.txt: {e}")
	
# ====================================================================== #
# Server functions
# ====================================================================== #

def build_protocol_response(message_type, payload=""):
	"""================================================================"""
	""" Build protocol response message, acording to defined format.   """
	"""================================================================"""
	return f"{message_type}:{payload}".encode(FORMAT)

def parse_protocol(data):
	"""================================================================"""
	""" Parse protocol message, acording to defined format.            """
	"""================================================================"""
	try:
		message = data.decode(FORMAT).strip()
		return message
	except UnicodeDecodeError:
		print("[ERROR] Failed to decode message.")
		return None

def handle_client(conn, addr):
	"""================================================================"""
	""" Handle client connection.                                      """
	"""================================================================"""
	print(f"[SOCKET] {addr} connected.")
	current_cp_id = None

	try:
		while True:
			data = conn.recv(1024)	# Read data from the message sent by the client
			if not data:
				break	# If there is no data, the client has disconnected
			
			message = parse_protocol(data)
			if message is None:
				continue

			print(f"[RECEIVED] {addr}: {message}")

			# [1] Process the message

			# [FORMATO] : TIPO_MENSAJE:ID:CAMPO1:CAMPO2:...
			parts = message.split(':')		# ":" is the delimiter
			msg_type = parts[0].upper()		

			# [2.1] Proceso de REGISTRO
			if msg_type == "REGISTRO" and len(parts) >= 3:
				# [FORMATO] : REGISTRO:ID:UBICACION:PRECIO
				current_cp_id = parts[1]
				location = parts[2]
				price = parts[3]
				# Validate and save the registration in the Database
				try:
					price_f = float(price)
					cp_registry[current_cp_id] = {
						"location": location,
						"price": price_f,
						"status": "ACTIVO",
						'address': addr
					}
					# [FORMATO ENVIADO] : ACEPTADO:ID
					response = build_protocol_response("ACEPTADO", current_cp_id)
					write_data_cp()
					print("[SERVER] Charging Point registered:", cp_registry[current_cp_id])
				except ValueError:
					response = build_protocol_response("RECHAZADO", "Invalid price format")
					print("[ERROR] Invalid price format from", addr)

				conn.sendall(response)

			# [2.2] Proceso de ACTUALIZACION DE ESTADO
			elif msg_type == "ESTADO" and len(parts) >= 3:
				# [FORMATO] : ESTADO:ID:ESTADO
				cp_id_report = parts[1]
				new_status = parts[2].upper()

				# Validate the ID and save the status in the Database
				if cp_id_report in cp_registry:
					if new_status in ["ACTIVO", "PARADO", "AVERIA", "SUMINISTRANDO", "DESCONECTADO"]:
						cp_registry[cp_id_report]["status"] = new_status
						response = build_protocol_response("ACTUALIZADO", cp_id_report)
						print(f"[SERVER] Status updated for {cp_id_report}: {new_status}")
					else:
						response = build_protocol_response("RECHAZADO", "Invalid status")
						print("[ERROR] Invalid status from", addr)
				else:
					response = build_protocol_response("RECHAZADO", "Unknown ID")
					print("[ERROR] Unknown ID from", addr)

				conn.sendall(response)

			# [2.3] Proceso de AUTENTICACION
			elif msg_type == "AUTENTICACION" and len(parts) >= 2:
				# [FORMATO] : AUTENTICACION:ID
				cp_id_auth = parts[1]
				if cp_id_auth in cp_registry:
					response = build_protocol_response("ACEPTADO", cp_id_auth)
					print(f"[SERVER] Charging Point {cp_id_auth} authenticated.")
				else:
					response = build_protocol_response("RECHAZADO", "Unknown ID")
					print("[ERROR] Unknown ID from", addr)
				conn.sendall(response)

			# [2.X] Mensaje no reconocido
			else:
				conn.sendall(build_protocol_response("RECHAZADO", "Unknown message type"))
	except ConnectionResetError:
		print(f"[SERVER] {addr} disconnected unexpectedly.")
	except Exception as e:
		print(f"[ERROR] Exception handling client {addr}: {e}")
	finally:
		if current_cp_id and current_cp_id in cp_registry:
			cp_registry[current_cp_id]["status"] = "DESCONECTADO"
			print(f"[SERVER] Charging Point {current_cp_id} marked as DESCONECTADO.")
		conn.close()
		print(f"[SERVER] {addr} connection closed.")


def start():
	"""================================================================"""
	""" Start the server and listen for incoming connections.		   """
	"""================================================================"""
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

# ====================================================================== #
# Main
# ====================================================================== #
if __name__ == "__main__":
	# 1. Inicialización de Socket (en el hilo principal)
	server = socket.socket()
	ADDR = (HOST, PORT_SOCKET)
	server.bind(ADDR)

	# 2. Hilo de Kafka (daemon)
	kafka_thread = threading.Thread(target=read_consumer, daemon=True)
	kafka_thread.start()
	
	print("[SERVER] Starting server...")
	print("[SERVER] Reading data from DataBase...")
	read_data_cp()
	
	# 3. Iniciar el servidor de Sockets
	try:
		start()
	except KeyboardInterrupt:
		print("\n[SERVER] Shutting down server...")
		write_data_cp()
		server.close()
		sys.exit()