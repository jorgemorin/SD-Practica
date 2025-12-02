import socket
import sys
import threading
import time
import os
<<<<<<< HEAD

SERVER = ""
PORT = 0
averia_activa = False
estado_actual = "DESCONOCIDO"
mensaje_status = ""
conectado = False

# Colores ANSI para una visualización cómoda
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BLUE = "\033[94m"
GRAY = "\033[90m"

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def render_screen(cp_id, engine_ip, engine_port):
    clear_screen()
    # Colores según estado
    if estado_actual == "ACTIVO":
        estado_color = f"{GREEN}{estado_actual}{RESET}"
    elif estado_actual == "AVERIADO":
        estado_color = f"{RED}{estado_actual}{RESET}"
    elif estado_actual == "SUMINISTRANDO":
        estado_color = f"{BLUE}{estado_actual}{RESET}"
    else:
        estado_color = f"{YELLOW}{estado_actual}{RESET}"

    print(f"{CYAN}{'═'*60}{RESET}")
    print(f"{BOLD}{' MONITOR DE PUNTO DE CARGA ':^60}{RESET}")
    print(f"{CYAN}{'═'*60}{RESET}")
    print(f"{BOLD}ID CP:{RESET} {cp_id}")
    print(f"{BOLD}CENTRAL:{RESET} {SERVER}:{PORT}")
    print(f"{BOLD}ENGINE :{RESET} {engine_ip}:{engine_port}")
    print(f"{CYAN}{'-'*60}{RESET}")
    print(f"{BOLD}ESTADO:{RESET} {estado_color}")
    print(f"{CYAN}{'-'*60}{RESET}")
    print(f"{BOLD}STATUS:{RESET} {mensaje_status}")
    print(f"{CYAN}{'═'*60}{RESET}")
    hora = time.strftime("%H:%M:%S")
    print(f"{GRAY}Última actualización: {hora}{RESET}")
=======

# --- 0. VISUALS ---
def _clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'

# --- 1. GLOBALES ---
averia_activa = False 
CENTRAL_SOCKET = None 
CENTRAL_LOCK = threading.Lock() 
APP_RUNNING = True # Nueva bandera para controlar el cierre limpio
>>>>>>> origin/Dani

# --- GLOBALES UI ---
UI_CP_ID = "N/A"
UI_CENTRAL_ADDR = "N/A"
UI_ENGINE_ADDR = "N/A"
UI_ENGINE_STATUS = "INICIANDO..."
UI_CENTRAL_STATUS = "DESCONECTADO" # Estado inicial
UI_LAST_LOG_LINE = ""
UI_LOCK = threading.Lock()

# --- 2. LÓGICA DE RED PERSISTENTE ---

def send_to_central(message: str, expect_reply: bool = True) -> str:
    """Envía un mensaje al socket persistente (thread-safe)."""
    global CENTRAL_SOCKET, UI_LAST_LOG_LINE, UI_CENTRAL_STATUS
    
    with CENTRAL_LOCK:
        if CENTRAL_SOCKET is None:
            return "ERROR" # Fallo silencioso si no hay red, para no bloquear
        
        try:
            CENTRAL_SOCKET.sendall(message.encode('utf-8'))
            if not expect_reply:
                return "OK"
            
            response = CENTRAL_SOCKET.recv(1024).decode('utf-8').strip()
            if not response:
                raise ConnectionResetError("Central cerró la conexión")
            return response
            
        except (socket.timeout, BrokenPipeError, ConnectionResetError, OSError) as e:
            # Si falla, cerramos el socket y el hilo network_manager se encargará de reconectar
            CENTRAL_SOCKET.close()
            CENTRAL_SOCKET = None 
            with UI_LOCK:
                UI_LAST_LOG_LINE = f"Conexión perdida: {e}"
                UI_CENTRAL_STATUS = "RECONECTANDO..."
            return "ERROR"
        except Exception as e:
            with UI_LOCK: UI_LAST_LOG_LINE = f"Error socket: {e}"
            return "ERROR"

def report_status_to_central(cp_id, status):
<<<<<<< HEAD
    global averia_activa, estado_actual, mensaje_status
    if status != estado_actual:
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
                        elif "CHARGING" in respuesta:
                            report_status_to_central(cp_id, "SUMINISTRANDO")
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
=======
    """Informa a Central de un cambio de estado. Funciona aunque Central esté caído."""
    global averia_activa, UI_CENTRAL_STATUS, UI_LAST_LOG_LINE
    
    # 1. Actualizar UI LOCAL (Siempre debe funcionar)
    with UI_LOCK:
        # Si la central está caída, mantenemos el estado de "RECONECTANDO" o "ERROR" en la UI de Central,
        # pero NO tocamos el estado del Engine, ese se gestiona en watchmen_thread.
        if CENTRAL_SOCKET is not None:
            if "PAUSED" in UI_ENGINE_STATUS: UI_CENTRAL_STATUS = "ACTIVO (Pausado)"
            elif status == "ACTIVO": UI_CENTRAL_STATUS = "ACTIVO (Conectado)"
            else: UI_CENTRAL_STATUS = f"{status} (Conectado)"
    
    # 2. Intentar enviar a Central (Solo si hay conexión)
    # Evitar spam: Solo enviar si el estado lógico (avería) cambia
    if (status == "AVERIADO" and not averia_activa) or (status == "ACTIVO" and averia_activa):
        
        if CENTRAL_SOCKET:
            with UI_LOCK:
                UI_LAST_LOG_LINE = f"--- Enviando reporte: {status} ---"
            send_to_central(f"ESTADO:{cp_id}:{status}", expect_reply=True)
        else:
            with UI_LOCK:
                UI_LAST_LOG_LINE = f"--- Pendiente de reportar: {status} (Sin Red) ---"
        
        averia_activa = (status == "AVERIADO")

def watchmen_thread(cp_id, engine_ip, engine_port):
    """
    Hilo vigilante que chequea la salud del Engine (local).
    AHORA ES INDEPENDIENTE DE LA CONEXIÓN A CENTRAL.
    """
    global UI_ENGINE_STATUS, UI_LAST_LOG_LINE
    
    while APP_RUNNING: # Se ejecuta siempre mientras la app esté viva
        try:
            # Intentar conectar con Engine
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                try:
                    s.connect((engine_ip, engine_port))
                except Exception:
                     # Si no conecta al Engine, reportamos avería y reintentamos loop
                    report_status_to_central(cp_id, "AVERIADO")
                    with UI_LOCK: UI_ENGINE_STATUS = "ERROR (NO CONECTADO)"
                    time.sleep(2)
                    continue

                # Bucle de vigilancia conectado al Engine
                while APP_RUNNING:
                    try:
                        s.sendall(b"HEALTH_CHECK")
                        resp = s.recv(1024).decode('utf-8')
                        if not resp: break # Engine cerró conexión
                        
                        if "KO" in resp:
                            report_status_to_central(cp_id, "AVERIADO")
                            with UI_LOCK: UI_ENGINE_STATUS = "ERROR (KO)"
                        elif "PAUSED" in resp:
                             report_status_to_central(cp_id, "ACTIVO")
                             with UI_LOCK: UI_ENGINE_STATUS = "OK (PAUSADO)"
                        else:
                            report_status_to_central(cp_id, "ACTIVO")
                            with UI_LOCK: UI_ENGINE_STATUS = "OK"
                    except Exception:
                        break # Error en el socket del engine, salir para reconectar
                    
                    time.sleep(1)
        
        except Exception as e:
            # Error genérico
            with UI_LOCK: UI_LAST_LOG_LINE = f"Watchman Error: {e}"
            time.sleep(2)

def network_manager_thread(server_ip, server_port, cp_id):
    """
    Hilo dedicado a mantener la conexión con Central.
    Si se cae, reconecta automáticamente sin afectar al resto.
    """
    global CENTRAL_SOCKET, UI_CENTRAL_STATUS, UI_LAST_LOG_LINE
    
    while APP_RUNNING:
        with CENTRAL_LOCK:
            current_socket = CENTRAL_SOCKET

        # 1. Si estamos conectados, enviamos PING (Keepalive)
        if current_socket is not None:
            if send_to_central(f"PING:{cp_id}", expect_reply=False) == "ERROR":
                # El send detectó error y puso CENTRAL_SOCKET a None
                continue 
            time.sleep(5)
        
        # 2. Si NO estamos conectados, intentamos conectar
        else:
            with UI_LOCK: UI_CENTRAL_STATUS = "CONECTANDO..."
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((server_ip, server_port))
                
                # Intentar Autenticar (Asumimos que ya se registró al inicio del programa)
                sock.sendall(f"AUTENTICACION:{cp_id}".encode('utf-8'))
                resp = sock.recv(1024).decode('utf-8').strip()

                if resp == f"ACEPTADO:{cp_id}":
                    with CENTRAL_LOCK: CENTRAL_SOCKET = sock
                    with UI_LOCK: 
                        UI_CENTRAL_STATUS = "ACTIVO (Conectado)"
                        UI_LAST_LOG_LINE = "Reconexión exitosa con Central."
                    
                    # Al reconectar, forzamos un reporte de estado actual
                    current_status = "ACTIVO" 
                    if "ERROR" in UI_ENGINE_STATUS: current_status = "AVERIADO"
                    # Usamos un thread timer breve para no bloquear aquí
                    threading.Timer(1.0, report_status_to_central, [cp_id, current_status]).start()

                else:
                    sock.close()
                    with UI_LOCK: UI_LAST_LOG_LINE = f"Rechazado por Central: {resp}"
                    time.sleep(10) # Espera larga si es rechazado

            except Exception as e:
                with UI_LOCK: 
                    UI_CENTRAL_STATUS = "ERROR (Sin Conexión)"
                    # UI_LAST_LOG_LINE = f"Buscando Central..." # Opcional: para no ensuciar el log
                time.sleep(5) # Reintentar en 5s

def initial_setup(server_ip, server_port, cp_id) -> bool:
    """Configuración inicial BLOQUEANTE (Registro/Login) antes de iniciar hilos."""
    global CENTRAL_SOCKET
    print(f"Conectando a Central {server_ip}:{server_port}...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((server_ip, server_port))
        
        sock.sendall(f"AUTENTICACION:{cp_id}".encode('utf-8'))
        resp = sock.recv(1024).decode('utf-8').strip()

        if resp == f"ACEPTADO:{cp_id}":
            print(f"{Colors.GREEN}Login correcto.{Colors.RESET}")
            sock.close() # Cerramos este socket temporal, el network_manager abrirá el persistente
            return True
        
        if resp == "RECHAZADO:":
            print(f"{Colors.YELLOW}CP no registrado.{Colors.RESET}")
            choice = input("¿Registrar ahora? (s/n): ").strip().lower()
            if choice == 's':
                loc = input("  -> Ubicación: ")
                pr = input("  -> Precio: ")
                sock.sendall(f"REGISTRO:{cp_id}:{loc}:{pr}".encode('utf-8'))
                reg_resp = sock.recv(1024).decode('utf-8').strip()
                if reg_resp == f"ACEPTADO:{cp_id}":
                    print(f"{Colors.GREEN}Registrado.{Colors.RESET}")
                    sock.close()
                    return True
            return False
            
    except Exception as e:
        print(f"{Colors.RED}Error inicial: {e}{Colors.RESET}")
        return False # Falló setup, pero permitimos arrancar en modo offline si se desea?
        # Para este ejemplo, si falla el inicio, asumimos que queremos reintentar en el bucle principal o salir.
        # Pero para que el Engine funcione siempre, vamos a devolver True simulado si queremos modo offline, 
        # pero mejor devolvemos False y que el usuario revise su IP.
    return False

# --- 3. UI ---
def main_ui_loop():
    """Bucle principal de la UI."""
    global UI_LAST_LOG_LINE
    last_log = ""
    
    while APP_RUNNING:
        _clear_screen()
        print(f"{Colors.CYAN}{Colors.BOLD}=== EV MONITOR (CP: {UI_CP_ID}) ==={Colors.RESET}")
        print(f"Central: {UI_CENTRAL_ADDR} | Engine: {UI_ENGINE_ADDR}")
        print("------------------------------------------------")
        
        with UI_LOCK:
             eng_st = UI_ENGINE_STATUS
             cen_st = UI_CENTRAL_STATUS
             if UI_LAST_LOG_LINE:
                 last_log = UI_LAST_LOG_LINE
                 UI_LAST_LOG_LINE = "" # Limpiar tras leer

        print(f"{Colors.BOLD}ENGINE : {Colors.RESET}", end="")
        if "OK" in eng_st: print(f"{Colors.GREEN}{eng_st}{Colors.RESET}")
        elif "PAUSADO" in eng_st: print(f"{Colors.YELLOW}{eng_st}{Colors.RESET}")
        elif "INICIANDO" in eng_st: print(f"{Colors.CYAN}{eng_st}{Colors.RESET}")
        else: print(f"{Colors.RED}{eng_st}{Colors.RESET}")

        print(f"{Colors.BOLD}CENTRAL: {Colors.RESET}", end="")
        if "ACTIVO" in cen_st: print(f"{Colors.GREEN}{cen_st}{Colors.RESET}")
        elif "CONECTANDO" in cen_st: print(f"{Colors.YELLOW}{cen_st}{Colors.RESET}")
        elif "DESCONECTADO" in cen_st: print(f"{Colors.RED}{cen_st}{Colors.RESET}")
        else: print(f"{Colors.RED}{cen_st}{Colors.RESET}")

        print("------------------------------------------------")
        print(f"{Colors.BOLD}LOG:{Colors.RESET} {last_log}")
        
        time.sleep(1)

# --- 4. MAIN ---
def __main__():
    global UI_CP_ID, UI_CENTRAL_ADDR, UI_ENGINE_ADDR, APP_RUNNING
    
    if len(sys.argv) != 6:
        print(f"Uso: python EV_CP_M.py <IP_CEN> <PORT_CEN> <IP_ENG> <PORT_ENG> <ID_CP>")
        return
    
    SERVER_IP, SERVER_PORT = sys.argv[1], int(sys.argv[2])
    eng_ip, eng_port, cp_id = sys.argv[3], int(sys.argv[4]), sys.argv[5]
    
    UI_CP_ID = cp_id
    UI_CENTRAL_ADDR = f"{SERVER_IP}:{SERVER_PORT}"
    UI_ENGINE_ADDR = f"{eng_ip}:{eng_port}"

    _clear_screen()
    # 1. Intentamos registro inicial (Interactiva)
    # Si falla la conexión inicial, preguntamos si continuar en modo "Offline" (solo Engine)
    if not initial_setup(SERVER_IP, SERVER_PORT, cp_id):
        print(f"{Colors.RED}No se pudo contactar con Central.{Colors.RESET}")
        print("Iniciando en modo MONITOR LOCAL (intentará reconectar en segundo plano)...")
        time.sleep(2)

    # 2. Arrancar Hilos
    # Hilo de Red (Se encarga de conectar, reconectar y ping)
    net_thread = threading.Thread(target=network_manager_thread, args=(SERVER_IP, SERVER_PORT, cp_id), daemon=True)
    net_thread.start()

    # Hilo Vigilante (Se encarga del Engine local)
    watch_thread = threading.Thread(target=watchmen_thread, args=(cp_id, eng_ip, eng_port), daemon=True)
    watch_thread.start()

    # 3. UI Loop (Bloqueante hasta Ctrl+C)
    try:
        main_ui_loop()
    except KeyboardInterrupt:
        APP_RUNNING = False
        print(f"\n{Colors.MAGENTA}Cerrando Monitor...{Colors.RESET}")
        sys.exit(0)
>>>>>>> origin/Dani

if __name__ == "__main__":
    __main__()
