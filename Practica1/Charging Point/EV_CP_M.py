#Socket:
#Debe comunicarse con Central para la autenticacion y autorizacion
import socket
import sys
import threading
import time
import os # Necesario para encontrar y modificar engine.conf

# --- 0. VISUALS (NUEVO) ---
def _clear_screen():
    """Limpia la pantalla de la terminal."""
    os.system('cls' if os.name == 'nt' else 'clear')

class Colors:
    """Clase para colores de terminal ANSI."""
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
averia_activa = False # Estado lógico
conectado = False

# --- NUEVO: Globales para la UI ---
UI_CP_ID = "N/A"
UI_CENTRAL_ADDR = "N/A"
UI_ENGINE_ADDR = "N/A"
UI_ENGINE_STATUS = "INICIANDO..." # e.g., "CONECTANDO", "OK", "ERROR"
UI_CENTRAL_STATUS = "DESCONOCIDO" # e.g., "ACTIVO", "AVERIADO"
UI_LAST_LOG_LINE = "" # Para mensajes de estado
UI_LOCK = threading.Lock() # Para actualizar las variables UI de forma segura


# --- (NUEVA FUNCIÓN) ---
def update_config_file(filename, key_to_update, new_value):
    """
    Actualiza un valor clave en el archivo de configuración, preservando el resto.
    Busca el archivo en el mismo directorio que el script.
    """
    # __file__ es la ruta del script actual. os.path.dirname() obtiene el directorio.
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Si se ejecuta como binario compilado (PyInstaller) __file__ puede no estar
    if not base_dir or not os.path.exists(base_dir):
        base_dir = os.getcwd() # Usar directorio actual como fallback
        
    config_path = os.path.join(base_dir, filename)
    
    new_lines = []
    key_found = False
    
    try:
        # Leer el archivo (si existe)
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                lines = f.readlines()
        else:
            print(f"{Colors.YELLOW}[WARN] El archivo '{config_path}' no existe. Se creará uno nuevo.{Colors.RESET}")
            lines = []
        
        # Modificar las líneas en memoria
        for line in lines:
            line_stripped = line.strip()
            if not line_stripped or line_stripped.startswith('#'):
                new_lines.append(line)
                continue
            
            try:
                # Separar clave y valor
                if '=' in line_stripped:
                    clave, valor = line_stripped.split('=', 1)
                    if clave.strip() == key_to_update:
                        new_lines.append(f"{clave.strip()}={new_value}\n")
                        key_found = True
                    else:
                        new_lines.append(line) # Conservar la línea original
                else:
                    new_lines.append(line) # Conservar líneas sin '='
            except ValueError:
                new_lines.append(line) # Conservar líneas mal formadas
        
        # Si la clave no existía, la añadimos al final
        if not key_found:
             new_lines.append(f"{key_to_update}={new_value}\n")

        # Escribir el archivo de nuevo
        with open(config_path, 'w') as f:
            f.writelines(new_lines)
            
        print(f"{Colors.GREEN}[INFO] Archivo '{filename}' actualizado: {key_to_update} = {new_value}{Colors.RESET}")
        return True

    except Exception as e:
        print(f"{Colors.RED}[ERROR-FATAL] No se pudo actualizar el archivo de configuración '{config_path}': {e}{Colors.RESET}")
        return False
# --- (FIN NUEVA FUNCIÓN) ---


def report_status_to_central(cp_id, status):
    """
    Notifica a CENTRAL si el estado de avería ha cambiado.
    Actualiza la variable global de UI 'UI_CENTRAL_STATUS'.
    """
    global averia_activa, UI_CENTRAL_STATUS, UI_LAST_LOG_LINE
    
    # Siempre actualizamos el estado visual que reporta el watchmen
    with UI_LOCK:
        UI_CENTRAL_STATUS = status

    # Comprueba si realmente hay un cambio de estado LÓGICO que notificar
    should_report = (status == "AVERIADO" and not averia_activa) or \
                    (status == "ACTIVO" and averia_activa)

    if should_report:
        with UI_LOCK:
            UI_LAST_LOG_LINE = f"--- [CAMBIO DE ESTADO] Notificando a CENTRAL: {status}. ---"
        send_to_central(f"ESTADO:{cp_id}:{status}")
        # Actualizamos el estado global LÓGICO
        averia_activa = (status == "AVERIADO")


def watchmen_thread(cp_id, engine_ip, engine_port):
    """
    Hilo que vigila el estado del Engine.
    Actualiza las variables globales de UI en lugar de imprimir.
    """
    global averia_activa, UI_ENGINE_STATUS, UI_LAST_LOG_LINE
    
    while True: # Bucle externo: se encarga de RECONECTAR si la conexión se cae.
        try:
            with UI_LOCK:
                UI_ENGINE_STATUS = "CONECTANDO..."
                UI_LAST_LOG_LINE = f"Intentando conectar con Engine en {engine_ip}:{engine_port}..."
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_engine:
                s_engine.settimeout(5) # Timeout de 5 seg para la conexión inicial
                s_engine.connect((engine_ip, engine_port))
                
                with UI_LOCK:
                    UI_LAST_LOG_LINE = "Conexión establecida con Engine."
                
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
                            with UI_LOCK:
                                UI_LAST_LOG_LINE = "Engine cerró la conexión. Reconectando..."
                            break # Rompe el bucle interno para forzar una reconexión
                        
                        if "KO" in respuesta:
                            # El Engine está conectado pero reporta fallo
                            report_status_to_central(cp_id, "AVERIADO")
                            with UI_LOCK:
                                UI_ENGINE_STATUS = "ERROR (KO)"
                        else:
                            # El Engine está conectado y responde OK
                            report_status_to_central(cp_id, "ACTIVO")
                            with UI_LOCK:
                                UI_ENGINE_STATUS = "OK"
                        
                        # 3. Esperar 1 segundo
                        time.sleep(1)
                        
                    except (socket.timeout, socket.error) as e:
                        with UI_LOCK:
                            UI_LAST_LOG_LINE = f"Error en health check: {e}. Reconectando..."
                        break # Rompe el bucle interno para forzar una reconexión

        except (socket.timeout, ConnectionRefusedError, socket.error) as e:
            with UI_LOCK:
                UI_ENGINE_STATUS = "ERROR (NO CONECTADO)"
                UI_LAST_LOG_LINE = f"No se pudo conectar con Engine: {e}"
            
            # Si hay cualquier error de conexión, reportamos avería
            report_status_to_central(cp_id, "AVERIADO")
            
            with UI_LOCK:
                UI_LAST_LOG_LINE = "Reintentando conexión a Engine en 5 segundos..."
            time.sleep(5) # Esperamos 5 segundos antes de intentar reconectar


def send_to_central(message):
    global conectado, UI_LAST_LOG_LINE
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
        print(f"{Colors.RED}[ERROR] No se pudo comunicar con CENTRAL: {e}{Colors.RESET}")
        with UI_LOCK:
            UI_LAST_LOG_LINE = f"ERROR: No se pudo comunicar con CENTRAL: {e}"
        conectado = False # Asegurarse de que 'conectado' es False si falla
        return f"ERROR_COMUNICACION: {e}"


def authenticate(cp_id):
    print(f"{Colors.YELLOW}Autenticando {cp_id} con Central...{Colors.RESET}")
    mensaje = f"AUTENTICACION:{cp_id}"
    respuesta = send_to_central(mensaje)
    
    if respuesta == f"ACEPTADO:{cp_id}":
        print(f"{Colors.GREEN}[OK] Autenticacion exitosa.{Colors.RESET}")
        return True
    else:
        print(f"{Colors.RED}[X] Autenticacion rechazada: {respuesta}{Colors.RESET}")
        return False

#CONEXION PARA REGISTRO CON CENTRAL
def register(cp_id, ubicacion, precio):
    print(f"{Colors.YELLOW}Enviando solicitud de registro para {cp_id} a Central...{Colors.RESET}")
    mensaje = f"REGISTRO:{cp_id}:{ubicacion}:{precio}"
    respuesta = send_to_central(mensaje)

    if "ACEPTADO" in respuesta:
        print(f"{Colors.GREEN}Registro exitoso.{Colors.RESET}")
        return True
    else:
        print(f"{Colors.RED}Registro fallido: {respuesta}{Colors.RESET}")
        return False


def main_ui_loop():
    """
    NUEVO: Bucle principal de la UI.
    Limpia y redibuja la pantalla cada segundo.
    """
    global UI_CP_ID, UI_CENTRAL_ADDR, UI_ENGINE_ADDR, \
           UI_ENGINE_STATUS, UI_CENTRAL_STATUS, UI_LAST_LOG_LINE

    log_line = "" # Para almacenar la última línea de log
    
    while True:
        _clear_screen()
        print(f"{Colors.CYAN}{Colors.BOLD}================================================{Colors.RESET}")
        print(f"{Colors.CYAN}{Colors.BOLD}  EV CHARGING POINT MONITOR (CP ID: {UI_CP_ID}) {Colors.RESET}")
        print(f"{Colors.CYAN}{Colors.BOLD}================================================{Colors.RESET}")
        
        print(f"{Colors.BOLD} 🎯 Central: {Colors.RESET}{UI_CENTRAL_ADDR}")
        print(f"{Colors.BOLD} ⚡ Engine:  {Colors.RESET}{UI_ENGINE_ADDR}")
        print("------------------------------------------------")

        with UI_LOCK:
            # Captura los strings de estado de forma segura
            engine_status_str = UI_ENGINE_STATUS
            central_status_str = UI_CENTRAL_STATUS
            if UI_LAST_LOG_LINE:
                log_line = UI_LAST_LOG_LINE # Captura la línea de log
                UI_LAST_LOG_LINE = "" # La limpia después de capturarla
        
        # --- Panel de Estado ---
        print(f"{Colors.BOLD} ESTADO ENGINE: {Colors.RESET}", end="")
        if engine_status_str == "OK":
            print(f"{Colors.GREEN}{engine_status_str}{Colors.RESET}")
        elif "CONECTANDO" in engine_status_str:
            print(f"{Colors.YELLOW}{engine_status_str}{Colors.RESET}")
        else:
            print(f"{Colors.RED}{engine_status_str}{Colors.RESET}")
        
        print(f"{Colors.BOLD} ESTADO CENTRAL: {Colors.RESET}", end="")
        if central_status_str == "ACTIVO":
            print(f"{Colors.GREEN}{central_status_str}{Colors.RESET}")
        elif central_status_str == "AVERIADO":
            print(f"{Colors.RED}{central_status_str}{Colors.RESET}")
        else:
            print(f"{Colors.YELLOW}{central_status_str}{Colors.RESET}")

        print("------------------------------------------------")
        
        # --- Panel de Log ---
        print(f"{Colors.BOLD}LOG:{Colors.RESET}")
        if log_line:
            print(f"{Colors.WHITE} {log_line}{Colors.RESET}")
        
        time.sleep(1) # Tasa de refresco de la UI


def __main__():
    global SERVER, PORT, conectado
    global UI_CP_ID, UI_CENTRAL_ADDR, UI_ENGINE_ADDR # Globales de UI

    if len(sys.argv) != 6:
        print(f"{Colors.RED}Uso: python EV_CP_M.py <IP_CENTRAL> <PUERTO_CENTRAL> <IP_ENGINE> <PUERTO_ENGINE> <ID_CP>{Colors.RESET}")
        return

    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    engine_ip = sys.argv[3]
    engine_port = int(sys.argv[4])
    CP_ID = sys.argv[5]
    
    # Actualizar las globales de UI
    UI_CP_ID = CP_ID
    UI_CENTRAL_ADDR = f"{SERVER}:{PORT}"
    UI_ENGINE_ADDR = f"{engine_ip}:{engine_port}"

    if authenticate(CP_ID):
        # Autenticación exitosa
        print(f"{Colors.GREEN}[OK] Punto de carga {CP_ID} autenticado.{Colors.RESET}")
        
    elif not conectado:
        # Si 'conectado' sigue en False, significa que send_to_central() falló.
        print(f"{Colors.RED}[X] No se pudo conectar con CENTRAL en {SERVER}:{PORT}. Abortando.{Colors.RESET}")
        return
        
    else:
        # Conectamos, pero la autenticación fue rechazada. Preguntamos para registrar.
        print(f"{Colors.YELLOW}[!] Autenticacion fallida para {CP_ID}. ¿Quieres registrarlo? (S/N){Colors.RESET}")
        response = input(f"{Colors.BOLD}> {Colors.RESET}").strip().lower()
        
        if response == 's':
            ubicacion = input(f" {Colors.CYAN}Ingrese la ubicacion del Punto de Carga: {Colors.RESET}")
            precio_str = input(f" {Colors.CYAN}Ingrese el precio por kWh (ej: 0.45): {Colors.RESET}") # Capturar como string
            
            try:
                # Validar que es un número antes de hacer nada
                float(precio_str)
            except ValueError:
                print(f"{Colors.RED}[ERROR] El precio debe ser un número. Registro cancelado.{Colors.RESET}")
                return

            print(f"{Colors.YELLOW}[INFO] Actualizando 'engine.conf' local con PRECIO_KWH={precio_str}...{Colors.RESET}")
            
            # Actualizar el archivo de configuración local del Engine
            if not update_config_file("engine.conf", "PRECIO_KWH", precio_str):
                print(f"{Colors.RED}[ERROR-FATAL] No se pudo actualizar 'engine.conf' local.{Colors.RESET}")
                print("El registro en Central continuará, pero el Engine deberá configurarse manualmente.")
            else:
                print(f"{Colors.GREEN}[INFO] 'engine.conf' local actualizado exitosamente.{Colors.RESET}")
            
            # Continuar con el registro en Central
            if not register(CP_ID, ubicacion, precio_str):
                print(f"{Colors.RED}Registro en Central fallido. Saliendo...{Colors.RESET}")
                return
            else:
                print(f"{Colors.GREEN}[OK] Punto de carga {CP_ID} registrado en Central exitosamente.{Colors.RESET}")
                
        else:
            print("Registro omitido. Saliendo...")
            return

    print(f"\n{Colors.GREEN}[INFO] Iniciando vigilancia continua del Engine...{Colors.RESET}")
    print(f"{Colors.YELLOW}Presione Ctrl+C para salir.{Colors.RESET}")
    time.sleep(2) # Pausa para leer el mensaje
    
    # Hilo vigilante para monitorizar el Engine
    watchmen = threading.Thread(
        target=watchmen_thread, 
        args=(CP_ID, engine_ip, engine_port), 
        daemon=True # El hilo vigilante se ejecuta como daemon
    )
    watchmen.start()

    try:
        # En lugar de dormir, iniciamos el bucle de la UI
        main_ui_loop()
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Cerrando Monitor...{Colors.RESET}")

if __name__ == "__main__":
    __main__()