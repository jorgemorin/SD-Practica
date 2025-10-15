#Permance a la espera hasta recibir una señal de Central
#Debe comunicarse con Monitor para recibir su ID y Monitor debe comunicarse con Central para validar el ID

import socket
import sys

#CONEXION PARA AUTENTICACION CON CENTRAL
def wait_for_service():
        try:
            # Crear y conectar el socket TCP
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.bind((HOST, PORT))
                server_socket.listen(1)
                print(f"[+] Esperando conexion en {HOST}:{PORT}")
                conn, addr = server_socket.accept()
                with conn:
                    print(f"[+] Conexion aceptada de {addr}")
    
                    # Recibir mensaje de servicio
                    mensaje = conn.recv(1024).decode('utf-8').strip()
                    
    
        except socket.error as e:
            print(f"[ERROR] Error de socket: {e}")
            return None
        except Exception as e:
            print(f"[ERROR] Excepcion inesperada: {e}")
            return None
