import socket
import sys
import threading
import time
import os

# --- 0. VISUALS ---
def _clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'

# --- 1. GLOBALES ---
SERVER = ""
PORT = 0
averia_activa = False
conectado = False

# --- GLOBALES UI ---
UI_CP_ID = "N/A"
UI_CENTRAL_ADDR = "N/A"
UI_ENGINE_ADDR = "N/A"
UI_ENGINE_STATUS = "INICIANDO..."
UI_CENTRAL_STATUS = "DESCONOCIDO"
UI_LAST_LOG_LINE = ""
UI_LOCK = threading.Lock()

def report_status_to_central(cp_id, status):
    global averia_activa, UI_CENTRAL_STATUS, UI_LAST_LOG_LINE
    with UI_LOCK:
        UI_CENTRAL_STATUS = status
    
    # Solo reportar si hay un cambio lógico relevante para Central
    if (status == "AVERIADO" and not averia_activa) or (status == "ACTIVO" and averia_activa):
        with UI_LOCK:
            UI_LAST_LOG_LINE = f"--- Notificando a CENTRAL: {status} ---"
        send_to_central(f"ESTADO:{cp_id}:{status}")
        averia_activa = (status == "AVERIADO")

def watchmen_thread(cp_id, engine_ip, engine_port):
    global UI_ENGINE_STATUS, UI_LAST_LOG_LINE
    while True:
        try:
            with UI_LOCK:
                UI_ENGINE_STATUS = "CONECTANDO..."
                UI_LAST_LOG_LINE = f"Conectando a {engine_ip}:{engine_port}..."
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((engine_ip, engine_port))
                with UI_LOCK:
                    UI_LAST_LOG_LINE = "Conectado a Engine."
                report_status_to_central(cp_id, "ACTIVO")
                
                while True:
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
                    time.sleep(1)
        except Exception as e:
            with UI_LOCK:
                UI_ENGINE_STATUS = "ERROR (NO CONECTADO)"
                UI_LAST_LOG_LINE = f"Error Engine: {e}"
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
            return s.recv(1024).decode('utf-8').strip()
    except Exception:
        conectado = False
        return "ERROR"

def authenticate(cp_id):
    print(f"{Colors.YELLOW}Autenticando...{Colors.RESET}")
    resp = send_to_central(f"AUTENTICACION:{cp_id}")
    return resp == f"ACEPTADO:{cp_id}"

def main_ui_loop():
    # CORRECCIÓN AQUÍ: Declarar global para poder modificarla
    global UI_LAST_LOG_LINE 
    
    last_log = ""
    while True:
        _clear_screen()
        print(f"{Colors.CYAN}{Colors.BOLD}=== EV MONITOR (CP: {UI_CP_ID}) ==={Colors.RESET}")
        print(f"Central: {UI_CENTRAL_ADDR} | Engine: {UI_ENGINE_ADDR}")
        print("------------------------------------------------")
        
        with UI_LOCK:
             eng_st = UI_ENGINE_STATUS
             cen_st = UI_CENTRAL_STATUS
             if UI_LAST_LOG_LINE:
                 last_log = UI_LAST_LOG_LINE
                 UI_LAST_LOG_LINE = ""

        print(f"{Colors.BOLD}ENGINE: {Colors.RESET}", end="")
        if "OK" in eng_st:
            print(f"{Colors.GREEN}{eng_st}{Colors.RESET}")
        elif "CONECTANDO" in eng_st:
            print(f"{Colors.YELLOW}{eng_st}{Colors.RESET}")
        else:
            print(f"{Colors.RED}{eng_st}{Colors.RESET}")

        print(f"{Colors.BOLD}CENTRAL: {Colors.RESET}", end="")
        if cen_st == "ACTIVO":
            print(f"{Colors.GREEN}{cen_st}{Colors.RESET}")
        else:
            print(f"{Colors.RED}{cen_st}{Colors.RESET}")

        print("------------------------------------------------")
        print(f"{Colors.BOLD}LOG:{Colors.RESET} {last_log}")
        time.sleep(1)

def __main__():
    global SERVER, PORT, UI_CP_ID, UI_CENTRAL_ADDR, UI_ENGINE_ADDR
    if len(sys.argv) != 6:
        print(f"Uso: python EV_CP_M.py <IP_CEN> <PORT_CEN> <IP_ENG> <PORT_ENG> <ID_CP>")
        return
    
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    eng_ip = sys.argv[3]
    eng_port = int(sys.argv[4])
    cp_id = sys.argv[5]
    
    UI_CP_ID = cp_id
    UI_CENTRAL_ADDR = f"{SERVER}:{PORT}"
    UI_ENGINE_ADDR = f"{eng_ip}:{eng_port}"

    if not authenticate(cp_id):
        if not conectado:
            print(f"{Colors.RED}Fallo conexión Central.{Colors.RESET}")
            return
        print(f"{Colors.YELLOW}Autenticación fallida. ¿Registrar? (s/n){Colors.RESET}")
        if input("> ").lower() == 's':
             loc = input("Ubicación: ")
             pr = input("Precio: ")
             resp = send_to_central(f"REGISTRO:{cp_id}:{loc}:{pr}")
             if "ACEPTADO" in resp:
                 print(f"{Colors.GREEN}Registrado.{Colors.RESET}")
             else:
                 return
        else:
            return

    threading.Thread(target=watchmen_thread, args=(cp_id, eng_ip, eng_port), daemon=True).start()

    try:
        main_ui_loop()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    __main__()