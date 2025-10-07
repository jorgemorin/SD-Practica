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

# ============================
# Configuración del servidor
# ============================
# HOST: Escuchar en todas las interfaces
HOST = '0.0.0.0'
# PORT_SOCKET: Puerto para la comuniación EV_CP_M <-> EV_Central
PORT_SOCKET = 65432
# FORMAT: Formato de codificación de los mensajes
FORMAT = 'utf-8'

# ============================
# Base de datos simulada
# ============================
cp_registry = {}

# ============================
def build_protocol_response(message_type, payload=""):
	return f"{message_type}:{payload}".encode(FORMAT)

def parse_protocol(data):
	try:
		message = data.decode(FORMAT).strip()
		return message
	except UnicodeDecodeError:
		print("[ERROR] Failed to decode message.")
		return None

def handle_client(conn, addr):
	print(f"[SOCKET] {addr} connected.")
	current_cp_id = None

	try:
		while True:
			# Recibir un mensaje del cliente
			data = conn.recv(1024).decode(FORMAT)
			if not data:
				# Si no hay datos, el cliente ha cerrado la conexión
				break

			message = parse_protocol(data)
			if message is None:
				continue

			print(f"[RECEIVED] {addr}: {message}")
			# [1] Procesar el mensaje recibido
			# FORMATO : TIPO_MENSAJE:ID:CAMPO1:CAMPO2:...
			parts = message.split(':')
			msg_type = parts[0].upper()

			# [2.1] Proceso de REGISTRO
			if msg_type == "REGISTRO" and len(parts) >= 4:
				# FORMATO : REGISTRO:ID:UBICACION:PRECIO
				current_cp_id = parts[1]
				location = parts[2]
				price = parts[3]

				# Validar y guardar en la base de datos
				try:
					price_f = float(price)
					cp_registry[current_cp_id] = {
						"location": location,
						"price": price_f,
						"status": "ACTIVO",
						'address': addr
					}
					# Formato enviado: ACEPTADO:ID
					reponse = build_protocol_response("ACEPTADO", current_cp_id)
					print("[INFO] Charging Point registered:", cp_registry[current_cp_id])
				except ValueError:
					response = build_protocol_response("RECHAZADO", "Invalid price format")
					print("[ERROR] Invalid price format from", addr)

				conn.sendall(reponse)

			# [2.2] Proceso de ACTUALIZACION DE ESTADO
			elif msg_type == "ESTADO" and len(parts) >= 3:
				# FORMATO : ESTADO:ID:ESTADO
				cp_id_report = parts[1]
				new_status = parts[2].upper()

				# Validar el ID y actualizar el estado
				if cp_id_report in cp_registry:
					if new_status in ["ACTIVO", "PARADO", "AVERIA", "SUMINISTRANDO", "DESCONECTADO"]:
						cp_registry[cp_id_report]["status"] = new_status
						response = build_protocol_response("ACTUALIZADO", cp_id_report)
						print(f"[INFO] Status updated for {cp_id_report}: {new_status}")
					else:
						response = build_protocol_response("RECHAZADO", "Invalid status")
						print("[ERROR] Invalid status from", addr)
				else:
					response = build_protocol_response("RECHAZADO", "Unknown ID")
					print("[ERROR] Unknown ID from", addr)

				conn.sendall(response)

			# [2.3] Proceso de AUTENTICACION
			elif msg_type == "AUTENTICACION" and len(parts) >= 2:
				# FORMATO : AUTENTICACION:ID
				cp_id_auth = parts[1]
				if cp_id_auth in cp_registry:
					response = build_protocol_response("ACEPTADO", cp_id_auth)
					print(f"[INFO] Charging Point {cp_id_auth} authenticated.")
				else:
					response = build_protocol_response("RECHAZADO", "Unknown ID")
					print("[ERROR] Unknown ID from", addr)
				conn.sendall(response)
			# [2.X] Mensaje no reconocido
			else:
				conn.sendall(build_protocol_response("RECHAZADO", "Unknown message type"))
	except ConnectionResetError:
		print(f"[SOCKET] {addr} disconnected unexpectedly.")
	except Exception as e:
		print(f"[ERROR] Exception handling client {addr}: {e}")
	finally:
		if current_cp_id and current_cp_id in cp_registry:
			cp_registry[current_cp_id]["status"] = "DESCONECTADO"
			print(f"[INFO] Charging Point {current_cp_id} marked as DESCONECTADO.")
		conn.close()
		print(f"[SOCKET] {addr} connection closed.")


def start():
	print(f"[INFO] Server listening on {HOST}:{PORT_SOCKET}")
	
	server.listen()
	num_conections = threading.active_count() - 1
	print("[INFO] Number of active connections: ", num_conections)

	while True:
		conn, addr = server.accept()
		num_conections = threading.active_count()
		thread = threading.Thread(target=handle_client, args=(conn, addr))
		thread.start()
		print(f"[INFO] Active connections: {num_conections}")

# ============================
if __name__ == "__main__":
	server = socket.socket()
	ADDR = (HOST, PORT_SOCKET)
	server.bind(ADDR)

	print("[INFO] Starting server...")
	start()