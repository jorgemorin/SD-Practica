
#Socket:
#Debe comunicarse con Central para la autenticacion y autorizacion
import socket
import sys
import threading
import time


SERVER = ""
PORT = 0
averia_activa = False


def send_to_engine():
    pass

def report_status_to_central(cp_id, status):
    
    #Notifica a CENTRAL si el estado de avería ha cambiado.

    global averia_activa
    
    # Comprueba si realmente hay un cambio de estado que notificar
    should_report = (status == "AVERIADO" and not averia_activa) or \
                    (status == "ACTIVO" and averia_activa)

    if should_report:
        print(f"--- [CAMBIO DE ESTADO] El CP ahora está {status}. Notificando a CENTRAL. ---")
        send_to_central(f"ESTADO:{cp_id}:{status}")
        # Actualizamos el estado global para no volver a notificar lo mismo
        averia_activa = (status == "AVERIADO")


def watchmen_thread(cp_id, engine_ip, engine_port):
    """
    Hilo que vigila el estado del Engine.
    Mantiene una conexión persistente para evitar spam de logs.
    """
    global averia_activa # Usamos la variable global que ya tenías
    
    while True: # Bucle externo: se encarga de RECONECTAR si la conexión se cae.
        try:
            print(f"[Monitor {cp_id}] Intentando conectar con Engine en {engine_ip}:{engine_port}...")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_engine:
                s_engine.settimeout(5) # Timeout de 5 seg para la conexión inicial
                s_engine.connect((engine_ip, engine_port))
                print(f"[Monitor {cp_id}] Conexión establecida con Engine.")
                s_engine.settimeout(3) # Timeout más corto para las operaciones
                
                # Si hemos conectado, significa que el CP está (o vuelve a estar) ACTIVO
                report_status_to_central(cp_id, "ACTIVO")

                while True: # Bucle interno: envía health checks por la conexión existente.
                    try:
                        # 1. Enviar health check
                        s_engine.sendall(b"HEALTH_CHECK")
                        
                        # 2. Esperar respuesta
                        respuesta = s_engine.recv(1024).decode('utf-8')
                        
                        if not respuesta:
                            print(f"[Monitor {cp_id}] Engine cerró la conexión. Reconectando...")
                            break # Rompe el bucle interno para forzar una reconexión
                        
                        if "KO" in respuesta:
                            # El Engine está conectado pero reporta fallo
                            report_status_to_central(cp_id, "AVERIADO")
                        else:
                            # El Engine está conectado y responde OK
                            report_status_to_central(cp_id, "ACTIVO")
                        
                        # 3. Esperar 1 segundo
                        time.sleep(1)
                        
                    except (socket.timeout, socket.error) as e:
                        print(f"[Monitor {cp_id}] Error durante el health check: {e}. Reconectando...")
                        break # Rompe el bucle interno para forzar una reconexión

        except (socket.timeout, ConnectionRefusedError, socket.error) as e:
            print(f"[Monitor {cp_id}] No se pudo conectar con Engine: {e}")
            # Si hay cualquier error de conexión, reportamos avería
            report_status_to_central(cp_id, "AVERIADO")
            # Esperamos 5 segundos antes de intentar reconectar
            print(f"[Monitor {cp_id}] Reintentando en 5 segundos...")
            time.sleep(5)


def send_to_central(message):
    global conectado
    #Función genérica para enviar un mensaje a CENTRAL y recibir una respuesta
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)  # Timeout de 5 segundos
            s.connect((SERVER, PORT))
            conectado = True
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
    mensaje = f"REGISTRO:{cp_id}:{ubicacion}:{precio}"
    respuesta = send_to_central(mensaje)

    if "ACEPTADO" in respuesta:
        print("Registro exitoso.")
        return True
    else:
        print(f"Registro fallido: {respuesta}")
        return False



def __main__():
    global SERVER, PORT, conectado
    conectado = False # Estado inicial

    if len(sys.argv) != 6:
        print("Uso: python EV_CP_M.py <IP_CENTRAL> <PUERTO_CENTRAL> <IP_ENGINE> <PUERTO_ENGINE> <ID_CP>")
        return

    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    engine_ip = sys.argv[3]
    engine_port = int(sys.argv[4])
    CP_ID = sys.argv[5]

    if authenticate(CP_ID):
        # Autenticación exitosa
        print(f"[OK] Punto de carga {CP_ID} autenticado.")
        
    elif not conectado:
        # Si 'conectado' sigue en False, significa que send_to_central() falló.
        print(f"[X] No se pudo conectar con CENTRAL en {SERVER}:{PORT}. Abortando.")
        return
        
    else:
        # Conectamos, pero la autenticación fue rechazada. Preguntamos para registrar.
        print(f"[!] Autenticacion fallida para {CP_ID}. ¿Quieres registrarlo? (S/N)")
        response = input().strip().lower()
        
        if response == 's':
            ubicacion = input("Ingrese la ubicacion del Punto de Carga: ")
            precio = input("Ingrese el precio por kWh: ")
            
            if not register(CP_ID, ubicacion, precio):
                print("Registro fallido. Saliendo...")
                return
            else:
                print(f"[OK] Punto de carga {CP_ID} registrado exitosamente.")
        else:
            print("Registro omitido. Saliendo...")
            return

    print(f"\n[INFO] Iniciando vigilancia continua del Engine en {engine_ip}:{engine_port}")
    
    # Hilo vigilante para monitorizar el Engine
    watchmen = threading.Thread(
        target=watchmen_thread, 
        args=(CP_ID, engine_ip, engine_port), 
        daemon=True
    )
    watchmen.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nCerrando Monitor...")

if __name__ == "__main__":
    __main__()