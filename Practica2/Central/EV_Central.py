import socket
import threading
import sqlite3
import os
import sys

# Configuración por defecto
DEFAULT_HOST = '0.0.0.0'
DEFAULT_PORT = 9092

# --- BÚSQUEDA AUTOMÁTICA DE LA BASE DE DATOS ---
POSSIBLE_PATHS = [
    'ev_charging.db',
    '../Registry/ev_charging.db',
    '../Practica2/Registry/ev_charging.db',
    'Registry/ev_charging.db'
]

DB_PATH = 'ev_charging.db'

for path in POSSIBLE_PATHS:
    if os.path.exists(path):
        DB_PATH = path
        break

def handle_client(conn, addr):
    print(f"[NUEVA CONEXIÓN] {addr}")
    try:
        connected = True
        while connected:
            msg = conn.recv(1024).decode('utf-8').strip()
            if not msg:
                break
            
            print(f"[{addr}] Mensaje recibido: {msg}")
            
            if msg.startswith("AUTENTICACION:"):
                parts = msg.split(":")
                if len(parts) >= 2:
                    cp_id = parts[1]
                    if verify_identity_in_db(cp_id):
                        print(f"✅ CP {cp_id} Aceptado (Encontrado en BD)")
                        conn.sendall(f"ACEPTADO:{cp_id}".encode('utf-8'))
                    else:
                        print(f"❌ CP {cp_id} Rechazado (No está en BD)")
                        conn.sendall(f"RECHAZADO:NO_REGISTRADO".encode('utf-8'))
                        connected = False
                else:
                    conn.sendall(b"ERROR:FORMATO_INCORRECTO")
            
            elif msg.startswith("ESTADO:"):
                conn.sendall(b"OK_ESTADO")
            
            elif msg.startswith("PING:"):
                pass 

    except Exception as e:
        print(f"[ERROR] {addr}: {e}")
    finally:
        conn.close()
        print(f"[DESCONEXIÓN] {addr} cerrada.")

def verify_identity_in_db(cp_id):
    try:
        with sqlite3.connect(DB_PATH) as conn_db:
            cursor = conn_db.cursor()
            cursor.execute("SELECT token FROM charging_points WHERE id = ?", (cp_id,))
            result = cursor.fetchone()
            if result:
                return True
            return False
    except Exception as e:
        print(f"Error consultando BD en {DB_PATH}: {e}")
        return False

def start_server():
    # Gestión de argumentos de línea de comandos
    host = DEFAULT_HOST
    port = DEFAULT_PORT
    
    # Lógica flexible para leer argumentos: python EV_Central.py [IP] [PORT] o [PORT]
    # Si pasaste "5002 172... 9092", intentaremos entenderlo
    args = sys.argv[1:]
    if len(args) > 0:
        # Si el primer argumento es un número, asumimos que es el PORT
        if args[0].isdigit():
            port = int(args[0])
            # Si hay un segundo argumento y es IP... (poco común, pero por si acaso)
        elif "." in args[0]: # Parece una IP
            host = args[0]
            if len(args) > 1 and args[1].isdigit():
                port = int(args[1])
        
        # Si usaste 3 argumentos (ej: api_port ip socket_port), tomamos el último como socket port
        if len(args) == 3 and args[2].isdigit():
             port = int(args[2])
             host = args[1]

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # --- FIX CRÍTICO: Evita el error "Address already in use" ---
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind((host, port))
    except OSError as e:
        print(f"❌ Error al iniciar en {host}:{port} -> {e}")
        print("💡 El puerto sigue ocupado. Prueba a ejecutar en terminal: 'fuser -k 9092/tcp'")
        return

    server.listen()
    print(f"--- CENTRAL ESCUCHANDO EN {host}:{port} ---")
    
    if os.path.exists(DB_PATH):
        print(f"--- ✅ BD ENCONTRADA EN: {DB_PATH} ---")
    else:
        print(f"--- ⚠️  ADVERTENCIA: NO SE ENCUENTRA LA BD ---")
        print(f"--- Buscado en: {POSSIBLE_PATHS} ---")
    
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()

if __name__ == "__main__":
    start_server()