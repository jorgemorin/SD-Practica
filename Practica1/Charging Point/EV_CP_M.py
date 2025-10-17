
#Socket:
#Debe comunicarse con Central para la autenticacion y autorizacion
#Debe comunicarse con Central para informar del estado del Punto de Carga

import socket
import sys


#CONEXION PARA AUTENTICACION CON CENTRAL
def authenticate(cp_id):
     try:
        # Crear y conectar el socket TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((SERVER, PORT))
            print(f"[+] Conectado al servidor {SERVER}:{PORT}")

            # Enviar mensaje de autenticacion
            mensaje = f"AUTENTICACION:{cp_id}"
            client_socket.sendall(mensaje.encode('utf-8'))
            print(f"Enviado: {mensaje}")

            # Recibir respuesta
            respuesta = client_socket.recv(1024).decode('utf-8').strip()
            print(f"Respuesta recibida: {respuesta}")

            # Procesar respuesta
            if respuesta == f"ACEPTADO:{cp_id}":
                print("[OK] Autenticacion exitosa.")
                return True
            elif respuesta.startswith("RECHAZADO:"):
                print(f"[X] Autenticacion rechazada: {respuesta}")
                return False
            else:
                print(f"[?] Respuesta desconocida del servidor: {respuesta}")
                return False

     except ConnectionRefusedError:
        print(f"[ERROR] No se pudo conectar al servidor {SERVER}:{PORT} (Conexion rechazada).")
        return False
     except socket.error as e:
        print(f"[ERROR] Error de socket: {e}")
        return False
     except Exception as e:
        print(f"[ERROR] Excepcion inesperada: {e}")
        return False

#CONEXION PARA REGISTRO CON CENTRAL
def register(ubicacion,precio):

    try:
        # Crear socket TCP
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Conectar al servidor
        client_socket.connect((SERVER, PORT))
        print("[+] Conectado al servidor.")
        mensaje = f"{"REGISTRO" +":"+ ubicacion +":"+ precio}"
        client_socket.sendall(mensaje.encode())
        respuesta = client_socket.recv(1024).decode()
        print(f"Respuesta del servidor: {respuesta}")
        if "ACEPTADO" in respuesta:
            parts = respuesta.split(':')		# ":" is the delimiter
            msg_type = parts[0].upper()	
            current_cp_id = parts[1]
            print("Registro exitoso.")
            return current_cp_id
        else:
            client_socket.close()
            return 0
    except Exception as e:
        print(f"[Error] {e}")


#CONEXION PARA REPORTAR ESTADO A ENGINE 
def report_status(cp_id,estado):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((SERVER, PORT))
            mensaje = f"SALUD:{cp_id}:{estado}"
            print(f"[Monitor {cp_id}] -> Enviando a Central: {mensaje}")
            client_socket.sendall(mensaje.encode('utf-8'))

            respuesta = client_socket.recv(1024).decode('utf-8')
            print(f"[Monitor {cp_id}] <- Respuesta: {respuesta}")

            if respuesta.startswith("CONFIRMADO"):
                print(f"[Monitor {cp_id}] Estado de salud confirmado por Central.")
            elif respuesta.startswith("RECHAZADO"):
                print(f"[Monitor {cp_id}] Reporte de salud rechazado: {respuesta}")
            else:
                print(f"[Monitor {cp_id}] Respuesta desconocida: {respuesta}")

    except Exception as e:
        print(f"[Error en reporte de salud] {e}")

def main():
    global SERVER, PORT

    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
        
    CP_ID = register("Calle Falsa 123","0.20")
    if CP_ID == 0:
        print("[X] Registro fallido. Saliendo.")
        return

if __name__ == "__main__":
    main()