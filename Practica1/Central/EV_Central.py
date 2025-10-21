# -*- coding: utf-8 -*-

# EV_Central

# Sockets:
# - Recepción de conexión de los Charging Points

# Recibe de los Charging Points:
# - ID del punto de recarga
# - Ubicación del punto de recarga
# - Precio que tiene el suministro
# - Estado (Activado/Parado/Suministrando/Averiado/Desconectado)

import socket
import threading
import sys

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

def read_data():
	"""================================================"""
	""" Reading data from DataBase.txt		           """
	"""================================================"""
	try:
		file = open("DataBase.txt", "r")
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
		print("[SERVER] Data loaded from DataBase.txt")
	
	except FileNotFoundError:
		print("[WARNING] DataBase.txt not found. Starting with empty database.")

def write_data():
	"""================================================"""
	""" Writing data to DataBase.txt		           """
	"""================================================"""
	try:
		file = open("DataBase.txt", "w")
		for cp_id, info in cp_registry.items():
			line = f"{cp_id}:{info['location']}:{info['price']}:{info['status']}\n"
			file.write(line)
		file.close()
		print("[SERVER] Data saved to DataBase.txt")
	
	except Exception as e:
		print(f"[ERROR] Failed to write to DataBase.txt: {e}")

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
					write_data()
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
	server = socket.socket()
	ADDR = (HOST, PORT_SOCKET)
	server.bind(ADDR)

	print("[SERVER] Starting server...")
	print("[SERVER] Reading data from DataBase...")
	read_data()

	start()