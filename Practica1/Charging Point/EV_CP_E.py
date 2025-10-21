#Permance a la espera hasta recibir una senal de Central
#Debe comunicarse con Monitor para recibir su ID y Monitor debe comunicarse con Central para validar el ID

from email import message
import socket
import sys

SERVER = ""
PORT = 0
SERVER_KAFKA=""
PORT_KAFKA = 0

#CONEXION PARA AUTENTICACION CON CENTRAL
def wait_for_monitor():
    """
    Engine espera una conexi�n directa del monitor (EV_CP_M).
    El monitor debe validar su ID con Central antes de llegar aqu�.
    """
    try:
        # Configura un nuevo socket para recibir al monitor
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_monitor:
            s_monitor.connect(("127.0.0.2", 5001))
            message=s_monitor.recv(1024)  
            if message == "AUTENTICADO":
                print(f"[OK] Conexi?n recibida de Monitor: {message.decode('utf-8')}")
                
    except Exception as e:
        print(f"[ERROR MONITOR] {e}")

def __main__():
    global SERVER, PORT 

    if len(sys.argv) != 3:
        print("Uso: python EV_CP_E.py <IP_kafka> <PUERTO_kafka>")
        return

    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    wait_for_monitor()
if __name__ == "__main__":
    __main__()