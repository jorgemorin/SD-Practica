
#Socket:
#Debe comunicarse con Central para la autenticacion y autorizacion
import socket
import sys
import threading
import time


SERVER = ""
PORT = 0
averia_activa = False


def report_status_to_central(cp_id, status):
    """
    Notifica a CENTRAL solo si el estado de avería ha cambiado.
    """
    global averia_activa
    
    # Comprueba si realmente hay un cambio de estado que notificar
    should_report = (status == "AVERIADO" and not averia_activa) or \
                    (status == "ACTIVADO" and averia_activa)

    if should_report:
        print(f"--- [CAMBIO DE ESTADO] El CP ahora está {status}. Notificando a CENTRAL. ---")
        send_to_central(f"ESTADO:{cp_id}:{status}")
        # Actualizamos el estado global para no volver a notificar lo mismo
        averia_activa = (status == "AVERIADO")


def watchmen_thread(cp_id, engine_ip, engine_port):
    """
    Este es el HILO VIGILANTE. Se ejecuta en bucle para comprobar la salud del Engine.
    """
    while True:
        try:
            # Intenta conectar con el Engine
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_engine:
                s_engine.settimeout(2)  # Si no responde en 2 segundos, consideramos que hay un fallo
                s_engine.connect((engine_ip, engine_port))
                
                # Si la conexión tiene éxito, significa que el Engine está VIVO.
                # Llamamos a nuestra función para que notifique a CENTRAL si veníamos de un estado de avería.
                report_status_to_central(cp_id, "ACTIVADO")
        
        except (socket.timeout, ConnectionRefusedError):
            # Si la conexión falla (timeout) o es rechazada, el Engine está CAÍDO.
            # Notificamos a CENTRAL (si no lo hemos hecho ya).
            report_status_to_central(cp_id, "AVERIADO")
        
        # Espera 1 segundo antes de la siguiente comprobación
        time.sleep(1)


def send_to_central(message):
    """
    Función genérica para enviar un mensaje a CENTRAL y recibir una respuesta.
    Esta reemplaza la lógica repetida en authenticate() y register().
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)  # Timeout de 5 segundos
            s.connect((SERVER, PORT))
            s.sendall(message.encode('utf-8'))
            response = s.recv(1024).decode('utf-8').strip()
            return response
    except Exception as e:
        print(f"[ERROR] No se pudo comunicar con CENTRAL: {e}")
        return f"ERROR_COMUNICACION: {e}"


def authenticate(cp_id):
    mensaje = f"AUTENTICACION:{cp_id}"
    respuesta = send_to_central(mensaje)
    
    if respuesta == f"ACEPTADO:{cp_id}":
        print("[OK] Autenticacion exitosa.")
        return True
    else:
        print(f"[X] Autenticacion rechazada: {respuesta}")
        return False

#CONEXION PARA REGISTRO CON CENTRAL
def register(cp_id, ubicacion, precio):
    """Usa la función genérica para enviar el registro."""
    # Arreglamos el formato del mensaje que estaba incorrecto
    mensaje = f"REGISTRO:{cp_id}:{ubicacion}:{precio}"
    respuesta = send_to_central(mensaje)

    if "ACEPTADO" in respuesta:
        print("Registro exitoso.")
        return True
    else:
        print(f"Registro fallido: {respuesta}")
        return False



def __main__():
    global SERVER, PORT 

    if len(sys.argv) != 6:
        print("Uso: python EV_CP_M.py <IP_CENTRAL> <PUERTO_CENTRAL> <IP_ENGINE> <PUERTO_ENGINE> <ID_CP>")
        return

    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    engine_ip = sys.argv[3]
    engine_port = int(sys.argv[4])
    CP_ID = sys.argv[5]

    if authenticate(CP_ID):
        print(f"[OK] Punto de carga {CP_ID} autenticado.")
        
    else:
        print(f"[X] Autenticacion fallida para {CP_ID}. ¿Quieres registrarlo? (S/N)")
        response = input()
        if response.lower() == 's':
            ubicacion = input("Ingrese la ubicacion del Punto de Carga: ")
            precio = input("Ingrese el precio por kWh: ")
            if not register(CP_ID, ubicacion, precio):
                print("Registro fallido. Saliendo...")
        elif response.lower() == 'n':
            print("Saliendo...")
            return
        else:
            print("Respuesta no valida. Saliendo...")
            return
        print(f"\n[INFO] Iniciando vigilancia continua del Engine en {engine_ip}:{engine_port}")
    
    #Hilo vigilante para monitorizar el Engine
    watchmen = threading.Thread(
        target=watchmen_thread, 
        args=(CP_ID, engine_ip, engine_port), 
        daemon=True #Segundo Plano
    )
    try:
        # Mantener el programa activo
        while True:
            time.sleep(10) 
    except KeyboardInterrupt:
        print("\nCerrando Monitor...")

if __name__ == "__main__":
    __main__()