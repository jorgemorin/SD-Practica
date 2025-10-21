#Permance a la espera hasta recibir una senal de Central
#Debe comunicarse con Monitor para recibir su ID y Monitor debe comunicarse con Central para validar el ID

import socket
import sys

#CONEXION PARA AUTENTICACION CON CENTRAL
def wait_for_monitor(cp_id):
    """
    Engine espera una conexi�n directa del monitor (EV_CP_M).
    El monitor debe validar su ID con Central antes de llegar aqu�.
    """
    try:
        # Configura un nuevo socket para recibir al monitor
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_monitor:
            s_monitor.bind(("127.0.0.2", "5001"))  # Usa un puerto distinto para el monitor
            s_monitor.listen(1)
            print(f"[ENGINE] Esperando conexi�n del Monitor (CP_ID={cp_id}) en puerto {PORT + 1} ...")

            conn, addr = s_monitor.accept()
            with conn:
                print(f"[ENGINE] Monitor conectado desde {addr}")

                # Recibir mensaje de autenticaci�n del monitor
                mensaje = conn.recv(1024).decode('utf-8').strip()
                print(f"[ENGINE] Mensaje del Monitor: {mensaje}")

                if mensaje == f"MONITOR:{cp_id}":
                    print(f"[ENGINE] Monitor {cp_id} autenticado correctamente.")
                    conn.sendall(f"ACEPTADO:{cp_id}".encode('utf-8'))
                else:
                    print("[ENGINE] ID de monitor no v�lido.")
                    conn.sendall(b"RECHAZADO:ID_invalido")
    except Exception as e:
        print(f"[ERROR MONITOR] {e}")

