import socket
import sys
import threading
import time
import os

SERVER = ""
PORT = 0
averia_activa = False
estado_actual = "DESCONOCIDO"
mensaje_status = ""
conectado = False

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def render_screen(cp_id, engine_ip, engine_port):
    clear_screen()
    print(f"╔══════════════════════════════════════════════════╗")
    print(f"║     MONITOR DE PUNTO DE CARGA - {cp_id}          ║")
    print(f"╠══════════════════════════════════════════════════╣")
    print(f"║ CENTRAL: {SERVER}:{PORT:<33}║")
    print(f"║ ENGINE : {engine_ip}:{engine_port:<33}║")
    print(f"╠══════════════════════════════════════════════════╣")
    print(f"║ ESTADO : {estado_actual:<40}║")
    print(f"╠══════════════════════════════════════════════════╣")
    print(f"║ {mensaje_status:<46}║")
    print(f"╚══════════════════════════════════════════════════╝")

def send_to_engine():
    pass

def report_status_to_central(cp_id, status):
    global averia_activa, estado_actual, mensaje_status
    should_report = (status == "AVERIADO" and not averia_activa) or (status == "ACTIVO" and averia_activa)
    if should_report:
        mensaje_status = f"El CP ahora está {status}. Notificando a CENTRAL..."
        send_to_central(f"ESTADO:{cp_id}:{status}")
        averia_activa = (status == "AVERIADO")
        estado_actual = status

def watchmen_thread(cp_id, engine_ip, engine_port):
    global averia_activa, estado_actual, mensaje_status
    while True:
        try:
            mensaje_status = f"Intentando conectar con Engine en {engine_ip}:{engine_port}..."
            render_screen(cp_id, engine_ip, engine_port)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_engine:
                s_engine.settimeout(5)
                s_engine.connect((engine_ip, engine_port))
                mensaje_status = "Conexión establecida con Engine."
                estado_actual = "ACTIVO"
                render_screen(cp_id, engine_ip, engine_port)
                s_engine.settimeout(3)
                report_status_to_central(cp_id, "ACTIVO")
                while True:
                    try:
                        s_engine.sendall(b"HEALTH_CHECK")
                        respuesta = s_engine.recv(1024).decode('utf-8')
                        if not respuesta:
                            mensaje_status = "Engine cerró la conexión. Reconectando..."
                            estado_actual = "AVERIADO"
                            render_screen(cp_id, engine_ip, engine_port)
                            break
                        if "KO" in respuesta:
                            report_status_to_central(cp_id, "AVERIADO")
                        else:
                            report_status_to_central(cp_id, "ACTIVO")
                        render_screen(cp_id, engine_ip, engine_port)
                        time.sleep(1)
                    except (socket.timeout, socket.error):
                        mensaje_status = "Error durante el health check. Reconectando..."
                        estado_actual = "AVERIADO"
                        render_screen(cp_id, engine_ip, engine_port)
                        break
        except (socket.timeout, ConnectionRefusedError, socket.error):
            mensaje_status = "No se pudo conectar con Engine. Reintentando..."
            estado_actual = "AVERIADO"
            render_screen(cp_id, engine_ip, engine_port)
            report_status_to_central(cp_id, "AVERIADO")
            time.sleep(5)

def send_to_central(message):
    global conectado
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((SERVER, PORT))
            conectado = True
            s.sendall(message.encode('utf-8'))
            response = s.recv(1024).decode('utf-8').strip()
            return response
    except Exception as e:
        return f"ERROR_COMUNICACION: {e}"

def authenticate(cp_id):
    mensaje = f"AUTENTICACION:{cp_id}"
    respuesta = send_to_central(mensaje)
    if respuesta == f"ACEPTADO:{cp_id}":
        return True
    else:
        return False

def register(cp_id, ubicacion, precio):
    mensaje = f"REGISTRO:{cp_id}:{ubicacion}:{precio}"
    respuesta = send_to_central(mensaje)
    if "ACEPTADO" in respuesta:
        return True
    else:
        return False

def __main__():
    global SERVER, PORT, conectado, estado_actual, mensaje_status
    conectado = False
    if len(sys.argv) != 6:
        print("Uso: python EV_CP_M.py <IP_CENTRAL> <PUERTO_CENTRAL> <IP_ENGINE> <PUERTO_ENGINE> <ID_CP>")
        return
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    engine_ip = sys.argv[3]
    engine_port = int(sys.argv[4])
    CP_ID = sys.argv[5]
    if not authenticate(CP_ID):
        if not conectado:
            mensaje_status = f"No se pudo conectar con CENTRAL en {SERVER}:{PORT}. Abortando."
            render_screen(CP_ID, engine_ip, engine_port)
            time.sleep(3)
            return
        else:
            mensaje_status = f"Autenticacion fallida para {CP_ID}."
            render_screen(CP_ID, engine_ip, engine_port)
            response = input("¿Quieres registrarlo? (S/N): ").strip().lower()
            if response == 's':
                ubicacion = input("Ingrese la ubicacion del Punto de Carga: ")
                precio = input("Ingrese el precio por kWh: ")
                if not register(CP_ID, ubicacion, precio):
                    mensaje_status = "Registro fallido. Saliendo..."
                    render_screen(CP_ID, engine_ip, engine_port)
                    time.sleep(3)
                    return
                else:
                    mensaje_status = f"Punto de carga {CP_ID} registrado exitosamente."
                    render_screen(CP_ID, engine_ip, engine_port)
            else:
                mensaje_status = "Registro omitido. Saliendo..."
                render_screen(CP_ID, engine_ip, engine_port)
                time.sleep(3)
                return
    watchmen = threading.Thread(target=watchmen_thread, args=(CP_ID, engine_ip, engine_port), daemon=True)
    watchmen.start()
    try:
        while True:
            render_screen(CP_ID, engine_ip, engine_port)
            time.sleep(1)
    except KeyboardInterrupt:
        clear_screen()
        print("Monitor finalizado.")

if __name__ == "__main__":
    __main__()
