from flask import Flask, request, jsonify
import sqlite3
import secrets
import ssl

app = Flask(__name__)
DB_NAME = 'ev_charging.db'

def init_db():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        # Creamos tabla si no existe. El 'token' es la credencial clave.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS charging_points (
                id TEXT PRIMARY KEY,
                location TEXT,
                token TEXT, 
                status TEXT
            )
        ''')
        conn.commit()

@app.route('/register', methods=['POST'])
def register():
    data = request.json
    cp_id = data.get('id')
    location = data.get('location')
    
    # 1. Generamos la CLAVE (Token) segura para este CP
    # Esta es la "credencial" que pide el enunciado 
    new_token = secrets.token_hex(16) 
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            # Guardamos la clave en BD para que Central pueda validarla luego
            cursor.execute('''
                INSERT OR REPLACE INTO charging_points (id, location, token, status)
                VALUES (?, ?, ?, 'REGISTERED')
            ''', (cp_id, location, new_token))
            conn.commit()
            
        print(f"CP Registrado: {cp_id} | Clave generada: {new_token}")
        
        # 2. Devolvemos la clave al CP
        return jsonify({'message': 'OK', 'token': new_token}), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    init_db()
    # Usamos tus certificados recien creados para HTTPS [cite: 77]
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('cert.pem', 'key.pem')
    
    print("🚀 Registry escuchando en puerto 5000 (HTTPS)...")
    app.run(host='0.0.0.0', port=5000, ssl_context=context)