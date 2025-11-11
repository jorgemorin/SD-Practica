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
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'

# --- 1. GLOBALES ---
averia_activa = False
CENTRAL_SOCKET = None
CENTRAL_LOCK = threading.Lock()
CP_ID_GLOBAL = "N/A"

# --- GLOBALES UI ---
UI_CP_ID = "N/A"
UI_CENTRAL_ADDR = "N/A"
UI_ENGINE_ADDR = "N/A"
UI_ENGINE_STATUS = "INICIANDO..."
UI_CENTRAL_STATUS = "CONECTANDO..."
UI_LAST_LOG_LINE = ""
UI_LOCK = threading.Lock()

# --- 2. LÓGICA DE RED PERSISTENTE ---

def send_to_central(message: str, expect_reply: bool = True) -> str:
    """Envía un mensaje al socket persistente de la Central."""
    global CENTRAL_SOCKET, UI_LAST_LOG_LINE, UI_CENTRAL_STATUS
    
    with CENTRAL_LOCK:
        if CENTRAL_SOCKET is None:
            with UI_LOCK:
                UI_LAST_LOG_LINE = "Error: Socket central no conectado."
                UI_CENTRAL_STATUS = "ERROR (Socket Nulo)"
            return "ERROR"
        
        try:
            CENTRAL_SOCKET.sendall(message.encode('utf-8'))
            if not expect_reply:
                return "OK" # Para PINGs
            
            response = CENTRAL_SOCKET.recv(1024).decode('utf-8').strip()
            if not response:
                raise ConnectionResetError("Central cerró la conexión")
            return response
            
        except (socket.timeout, BrokenPipeError, ConnectionResetError, OSError) as e:
            with UI_LOCK:
                UI_LAST_LOG_LINE = f"Error de conexión con Central: {e}"
                UI_CENTRAL_STATUS = "ERROR (Socket Roto)"
            CENTRAL_SOCKET = None # Marcar el socket como muerto
            return "ERROR"
        except Exception as e:
            with UI_LOCK: UI_LAST_LOG_LINE = f"Error socket inesperado: {e}"
            return "ERROR"

def ping_thread(cp_id: str):
    """Mantiene la conexión viva y permite a Central detectar la muerte."""
    while CENTRAL_SOCKET is not None:
        time.sleep(5)
        if CENTRAL_SOCKET is None: break
            
        if send_to_central(f"PING:{cp_id}", expect_reply=False) == "ERROR":
            with UI_LOCK:
                UI_LAST_LOG_LINE = "Fallo al enviar PING (reconectando...)"

def report_status_to_central(cp_id, status):
    """Reporta el estado del Engine (Averiado/Activo) a la Central."""
    global averia_activa, UI_CENTRAL_STATUS, UI_LAST_LOG_LINE
    
    with UI_LOCK:
        if "PAUSED" in UI_ENGINE_STATUS: UI_CENTRAL_STATUS = "ACTIVO (Pausado)"
        elif status == "ACTIVO": UI_CENTRAL_STATUS = "ACTIVO"
        else: UI_CENTRAL_STATUS = status
    
    if (status == "AVERIADO" and not averia_activa) or (status == "ACTIVO" and averia_activa):
        with UI_LOCK:
            UI_LAST_LOG_LINE = f"--- Notificando a CENTRAL: {status} ---"
        
        send_to_central(f"ESTADO:{cp_id}:{status}", expect_reply=True)
        averia_activa = (status == "AVERIADO")

def watchmen_thread(cp_id, engine_ip, engine_port):
    """Hilo que vigila el Engine y reporta a la Central si este falla."""
    global UI_ENGINE_STATUS, UI_LAST_LOG_LINE
    while CENTRAL_SOCKET is not None: # Si la Central se cae, este hilo muere
        try:
            with UI_LOCK:
                UI_ENGINE_STATUS = "CONECTANDO..."
                UI_LAST_LOG_LINE = f"Conectando a {engine_ip}:{engine_port}..."
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((engine_ip, engine_port))
                with UI_LOCK:
                    UI_LAST_LOG_LINE = "Conectado a Engine."
                
                # Bucle de vigilancia del Engine
                while CENTRAL_SOCKET is not None:
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

# ---
# --- FUNCIÓN DE CONEXIÓN Y REGISTRO (CORREGIDA) ---
# ---
def connect_or_register(server_ip, server_port, cp_id) -> bool:
    """
    Intenta conectar y autenticar. Si es rechazado,
    pregunta al usuario si desea registrar el CP.
    Esta función es BLOQUEANTE y usa input().
    """
    global CENTRAL_SOCKET, UI_CENTRAL_STATUS, UI_LAST_LOG_LINE
    
    with UI_LOCK: UI_CENTRAL_STATUS = "CONECTANDO..."
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((server_ip, server_port))
        
        # Guardamos el socket globalmente
        with CENTRAL_LOCK:
            CENTRAL_SOCKET = sock
        
        # 1. Intentar Autenticar
        print(f"{Colors.YELLOW}Autenticando {cp_id}...{Colors.RESET}")
        CENTRAL_SOCKET.sendall(f"AUTENTICACION:{cp_id}".encode('utf-8'))
        resp = CENTRAL_SOCKET.recv(1024).decode('utf-8').strip()

        if resp == f"ACEPTADO:{cp_id}":
            with UI_LOCK: UI_CENTRAL_STATUS = "ACTIVO"
            print(f"{Colors.GREEN}Autenticación exitosa.{Colors.RESET}")
            return True
        
        # 2. Si es Rechazado -> Preguntar para Registrar
        if resp == "RECHAZADO:":
            _clear_screen()
            print(f"{Colors.YELLOW}ADVERTENCIA: CP '{cp_id}' no está registrado en la Central.{Colors.RESET}")
            
            try:
                choice = input("¿Desea registrar este CP ahora? (s/n): ").strip().lower()
            except EOFError:
                choice = "n"

            if choice == 's':
                loc = input("  -> Ubicación (ej: C/Italia 5): ")
                pr = input("  -> Precio (ej: 0.54): ")
                
                print("Enviando registro...")
                CENTRAL_SOCKET.sendall(f"REGISTRO:{cp_id}:{loc}:{pr}".encode('utf-8'))
                reg_resp = CENTRAL_SOCKET.recv(1024).decode('utf-8').strip()

                if reg_resp == f"ACEPTADO:{cp_id}":
                    print(f"{Colors.GREEN}Registro exitoso.{Colors.RESET}")
                    with UI_LOCK: UI_CENTRAL_STATUS = "ACTIVO"
                    return True
                else:
                    print(f"{Colors.RED}El registro falló (Respuesta: {reg_resp}).{Colors.RESET}")
                    with CENTRAL_LOCK: CENTRAL_SOCKET.close(); CENTRAL_SOCKET = None
                    return False
            else:
                print("Registro cancelado.")
                with CENTRAL_LOCK: CENTRAL_SOCKET.close(); CENTRAL_SOCKET = None
                return False
        
        # Si la respuesta no es ACEPTADO ni RECHAZADO:
        print(f"{Colors.RED}Error de protocolo inesperado: {resp}{Colors.RESET}")
        with CENTRAL_LOCK: CENTRAL_SOCKET.close(); CENTRAL_SOCKET = None
        return False
            
    except Exception as e:
        with UI_LOCK:
            UI_LAST_LOG_LINE = f"Fallo al conectar/autenticar: {e}"
            UI_CENTRAL_STATUS = "ERROR (Conexión)"
        with CENTRAL_LOCK:
            CENTRAL_SOCKET = None
        return False

# --- 3. UI (CORREGIDA) ---
def main_ui_loop():
    """Bucle principal de la UI. Se ejecuta en el hilo principal."""
    # ---
    # --- CORRECCIÓN DEL SYNTAXERROR ---
    # ---
    global UI_LAST_LOG_LINE, CENTRAL_SOCKET
    # ---
    # --- FIN DE LA CORRECCIÓN ---
    # ---
    last_log = ""
    
    while CENTRAL_SOCKET is not None:
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
        if "OK" in eng_st: print(f"{Colors.GREEN}{eng_st}{Colors.RESET}")
        elif "CONECTANDO" in eng_st: print(f"{Colors.YELLOW}{eng_st}{Colors.RESET}")
        else: print(f"{Colors.RED}{eng_st}{Colors.RESET}")

        print(f"{Colors.BOLD}CENTRAL: {Colors.RESET}", end="")
        if "ACTIVO" in cen_st: print(f"{Colors.GREEN}{cen_st}{Colors.RESET}")
        elif "CONECTANDO" in cen_st: print(f"{Colors.YELLOW}{cen_st}{Colors.RESET}")
        else: print(f"{Colors.RED}{cen_st}{Colors.RESET}")

        print("------------------------------------------------")
        print(f"{Colors.BOLD}LOG:{Colors.RESET} {last_log}")
        
        time.sleep(1)

# ---
# --- 4. MAIN (CORREGIDO) ---
# ---
def __main__():
    global UI_CP_ID, UI_CENTRAL_ADDR, UI_ENGINE_ADDR, CP_ID_GLOBAL, CENTRAL_SOCKET, averia_activa
    
    if len(sys.argv) != 6:
        print(f"Uso: python EV_CP_M.py <IP_CEN> <PORT_CEN> <IP_ENG> <PORT_ENG> <ID_CP>")
        return
    
    SERVER_IP, SERVER_PORT = sys.argv[1], int(sys.argv[2])
    eng_ip, eng_port, cp_id = sys.argv[3], int(sys.argv[4]), sys.argv[5]
    
    UI_CP_ID, CP_ID_GLOBAL = cp_id, cp_id
    UI_CENTRAL_ADDR = f"{SERVER_IP}:{SERVER_PORT}"
    UI_ENGINE_ADDR = f"{eng_ip}:{eng_port}"

    # Bucle principal de conexión/reconexión
    while True:
        if CENTRAL_SOCKET is None:
            _clear_screen()
            print(f"Iniciando Monitor para {Colors.BOLD}{cp_id}{Colors.RESET}...")
            
            # 1. Conectar o Registrar (Bloqueante, usa input())
            if connect_or_register(SERVER_IP, SERVER_PORT, cp_id):
                # 2. Si tiene éxito, iniciar hilos de trabajo
                print(f"{Colors.GREEN}Conexión establecida. Iniciando servicios...{Colors.RESET}")
                
                threading.Thread(target=ping_thread, args=(cp_id,), daemon=True).start()
                threading.Thread(target=watchmen_thread, args=(cp_id, eng_ip, eng_port), daemon=True).start()

                # 3. El hilo principal se convierte en la UI
                main_ui_loop() # Esto bloquea hasta que el socket muere

                print(f"{Colors.RED}Socket central desconectado. Reiniciando...{Colors.RESET}")
                averia_activa = False # Resetear estado de avería al reconectar
            
            else:
                # Si el registro/conexión falla (p.ej. usuario dice 'n')
                print(f"{Colors.RED}Fallo de conexión/registro. Reintentando en 5 segundos...{Colors.RESET}")
                time.sleep(5)
        
        time.sleep(1) # Pequeña pausa en el bucle externo

if __name__ == "__main__":
    try:
        __main__()
    except KeyboardInterrupt:
        print(f"\n{Colors.MAGENTA}Cerrando Monitor...{Colors.RESET}")
        if CENTRAL_SOCKET:
            CENTRAL_SOCKET.close()
            
            