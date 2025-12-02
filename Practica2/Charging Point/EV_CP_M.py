import socket
import sys
import threading
import time
import os
import requests  # <--- NUEVO: Para la API REST
import urllib3   # <--- NUEVO: Para gestionar alertas de SSL

# Desactivar advertencias de certificados autofirmados
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
APP_RUNNING = True 

# --- NUEVO: GLOBALES DE SEGURIDAD FASE 1 ---
AUTH_TOKEN = None  # Aquí guardaremos la clave que nos de el Registry
REGISTRY_URL = "https://localhost:5000/register" # URL por defecto del Registry

# --- GLOBALES UI ---
UI_CP_ID = "N/A"
UI_CENTRAL_ADDR = "N/A"
UI_ENGINE_ADDR = "N/A"
UI_ENGINE_STATUS = "INICIANDO..."
UI_CENTRAL_STATUS = "DESCONECTADO" 
UI_LAST_LOG_LINE = ""
UI_LOCK = threading.Lock()

# --- 2. LÓGICA DE REGISTRO HTTPS (FASE 1) ---
def registrar_en_registry_https(cp_id, location="Alicante"):
    """
    Intenta registrarse en el EV_Registry vía API REST segura.
    Devuelve True si consigue el token, False si falla.
    """
    global AUTH_TOKEN
    print(f"{Colors.CYAN}--- FASE 1: Contactando con Registry (HTTPS) ---{Colors.RESET}")
    
    payload = {"id": cp_id, "location": location}
    
    try:
        # verify=False es CLAVE para certificados autofirmados
        response = requests.post(REGISTRY_URL, json=payload, verify=False, timeout=5)
        
        if response.status_code == 201:
            data = response.json()
            AUTH_TOKEN = data.get('token')
            print(f"{Colors.GREEN}✅ Registro Exitoso.{Colors.RESET}")
            print(f"{Colors.YELLOW}🔑 Token Guardado: {AUTH_TOKEN}{Colors.RESET}")
            return True
        else:
            print(f"{Colors.RED}❌ Registry rechazó el registro: {response.text}{Colors.RESET}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"{Colors.RED}❌ Error: No se encuentra EV_Registry en {REGISTRY_URL}{Colors.RESET}")
        return False
    except Exception as e:
        print(f"{Colors.RED}❌ Error inesperado en registro: {e}{Colors.RESET}")
        return False


# --- 3. LÓGICA DE RED PERSISTENTE ---

def send_to_central(message: str, expect_reply: bool = True) -> str:
    global CENTRAL_SOCKET, UI_LAST_LOG_LINE, UI_CENTRAL_STATUS
    
    with CENTRAL_LOCK:
        if CENTRAL_SOCKET is None:
            return "ERROR"
        
        try:
            CENTRAL_SOCKET.sendall(message.encode('utf-8'))
            if not expect_reply:
                return "OK"
            
            response = CENTRAL_SOCKET.recv(1024).decode('utf-8').strip()
            if not response:
                raise ConnectionResetError("Central cerró la conexión")
            return response
            
        except Exception as e:
            CENTRAL_SOCKET.close()
            CENTRAL_SOCKET = None 
            with UI_LOCK:
                UI_LAST_LOG_LINE = f"Conexión perdida: {e}"
                UI_CENTRAL_STATUS = "RECONECTANDO..."
            return "ERROR"

def report_status_to_central(cp_id, status):
    global averia_activa, UI_CENTRAL_STATUS, UI_LAST_LOG_LINE
    
    with UI_LOCK:
        if CENTRAL_SOCKET is not None:
            if "PAUSED" in UI_ENGINE_STATUS: UI_CENTRAL_STATUS = "ACTIVO (Pausado)"
            elif status == "ACTIVO": UI_CENTRAL_STATUS = "ACTIVO (Conectado)"
            else: UI_CENTRAL_STATUS = f"{status} (Conectado)"
    
    if (status == "AVERIADO" and not averia_activa) or (status == "ACTIVO" and averia_activa):
        if CENTRAL_SOCKET:
            with UI_LOCK: UI_LAST_LOG_LINE = f"--- Enviando reporte: {status} ---"
            # TODO: En el futuro, aquí usaremos AUTH_TOKEN para cifrar
            send_to_central(f"ESTADO:{cp_id}:{status}", expect_reply=True)
        else:
            with UI_LOCK: UI_LAST_LOG_LINE = f"--- Pendiente: {status} (Sin Red) ---"
        averia_activa = (status == "AVERIADO")

def watchmen_thread(cp_id, engine_ip, engine_port):
    global UI_ENGINE_STATUS, UI_LAST_LOG_LINE
    
    while APP_RUNNING:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                try:
                    s.connect((engine_ip, engine_port))
                except Exception:
                    report_status_to_central(cp_id, "AVERIADO")
                    with UI_LOCK: UI_ENGINE_STATUS = "ERROR (NO CONECTADO)"
                    time.sleep(2)
                    continue

                while APP_RUNNING:
                    try:
                        s.sendall(b"HEALTH_CHECK")
                        resp = s.recv(1024).decode('utf-8')
                        if not resp: break
                        
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
                        break
                    time.sleep(1)
        except Exception as e:
            with UI_LOCK: UI_LAST_LOG_LINE = f"Watchman Error: {e}"
            time.sleep(2)

def network_manager_thread(server_ip, server_port, cp_id):
    global CENTRAL_SOCKET, UI_CENTRAL_STATUS, UI_LAST_LOG_LINE
    
    while APP_RUNNING:
        with CENTRAL_LOCK:
            current_socket = CENTRAL_SOCKET

        if current_socket is not None:
            if send_to_central(f"PING:{cp_id}", expect_reply=False) == "ERROR":
                continue 
            time.sleep(5)
        else:
            with UI_LOCK: UI_CENTRAL_STATUS = "CONECTANDO..."
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((server_ip, server_port))
                
                # AHORA: Usamos el ID. En la Fase 2 aquí enviaremos el Token cifrado.
                sock.sendall(f"AUTENTICACION:{cp_id}".encode('utf-8'))
                resp = sock.recv(1024).decode('utf-8').strip()

                if resp == f"ACEPTADO:{cp_id}":
                    with CENTRAL_LOCK: CENTRAL_SOCKET = sock
                    with UI_LOCK: 
                        UI_CENTRAL_STATUS = "ACTIVO (Conectado)"
                        UI_LAST_LOG_LINE = "Reconexión exitosa con Central."
                    
                    current_status = "ACTIVO" 
                    if "ERROR" in UI_ENGINE_STATUS: current_status = "AVERIADO"
                    threading.Timer(1.0, report_status_to_central, [cp_id, current_status]).start()

                else:
                    sock.close()
                    with UI_LOCK: UI_LAST_LOG_LINE = f"Rechazado por Central: {resp}"
                    time.sleep(10)

            except Exception as e:
                with UI_LOCK: UI_CENTRAL_STATUS = "ERROR (Sin Conexión)"
                time.sleep(5)

# --- 4. SETUP INICIAL MODIFICADO ---
def initial_setup(server_ip, server_port, cp_id) -> bool:
    """
    1. Registra en Registry (HTTPS).
    2. Prueba conexión a Central (Socket).
    """
    
    # 1. REGISTRO (Nueva lógica Fase 1)
    if not registrar_en_registry_https(cp_id):
        # Si falla el registro, preguntamos si queremos seguir en modo offline
        # (Esto es útil si el Registry está caído pero quieres probar el Engine)
        print(f"{Colors.YELLOW}No se pudo obtener el TOKEN del Registry.{Colors.RESET}")
        choice = input("¿Intentar arrancar sin Token (modo offline/test)? (s/n): ")
        if choice.lower() != 's':
            return False
    
    # 2. CONEXIÓN A CENTRAL (Lógica antigua de prueba)
    print(f"Probando conexión a Central {server_ip}:{server_port}...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((server_ip, server_port))
        
        sock.sendall(f"AUTENTICACION:{cp_id}".encode('utf-8'))
        resp = sock.recv(1024).decode('utf-8').strip()

        if resp == f"ACEPTADO:{cp_id}":
            print(f"{Colors.GREEN}Login inicial en Central correcto.{Colors.RESET}")
            sock.close() 
            return True
        else:
            print(f"{Colors.RED}Central rechazó la conexión (¿Quizás la Central no tiene BD actualizada?).{Colors.RESET}")
            # En la vida real aquí fallaríamos, pero para lab permitimos seguir
            return True 
            
    except Exception as e:
        print(f"{Colors.RED}No se pudo conectar a Central: {e}{Colors.RESET}")
        return True # Permitimos arrancar para que funcione el Watchman local

# --- UI ---
def main_ui_loop():
    global UI_LAST_LOG_LINE
    last_log = ""
    
    while APP_RUNNING:
        _clear_screen()
        print(f"{Colors.CYAN}{Colors.BOLD}=== EV MONITOR (CP: {UI_CP_ID}) ==={Colors.RESET}")
        print(f"Central: {UI_CENTRAL_ADDR} | Engine: {UI_ENGINE_ADDR}")
        print(f"Registry: {REGISTRY_URL}")
        
        # Mostramos si tenemos Token o no
        token_display = "SI (Guardado)" if AUTH_TOKEN else "NO"
        print(f"Token de Seguridad: {Colors.MAGENTA}{token_display}{Colors.RESET}")
        print("------------------------------------------------")
        
        with UI_LOCK:
             eng_st = UI_ENGINE_STATUS
             cen_st = UI_CENTRAL_STATUS
             if UI_LAST_LOG_LINE:
                 last_log = UI_LAST_LOG_LINE
                 UI_LAST_LOG_LINE = ""

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

# --- MAIN ---
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
    
    # 1. SETUP: Registro HTTPS + Test Central
    if not initial_setup(SERVER_IP, SERVER_PORT, cp_id):
        print("Saliendo...")
        sys.exit(1)
        
    print("Iniciando monitorización...")
    time.sleep(1)

    # 2. Arrancar Hilos
    net_thread = threading.Thread(target=network_manager_thread, args=(SERVER_IP, SERVER_PORT, cp_id), daemon=True)
    net_thread.start()

    watch_thread = threading.Thread(target=watchmen_thread, args=(cp_id, eng_ip, eng_port), daemon=True)
    watch_thread.start()

    # 3. UI Loop
    try:
        main_ui_loop()
    except KeyboardInterrupt:
        APP_RUNNING = False
        print(f"\n{Colors.MAGENTA}Cerrando Monitor...{Colors.RESET}")
        sys.exit(0)

if __name__ == "__main__":
    __main__()
