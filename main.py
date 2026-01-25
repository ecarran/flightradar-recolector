import os
import time
import pytz
import gspread
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# --- CONFIGURACIÓN ---
IATA_CODE = "MAD"
ZONA_HORARIA = pytz.timezone("Europe/Madrid")
GOOGLE_JSON = "service_account.json" 
SPREADSHEET_NAME = "Barajas_Master_Data"

app = FastAPI()
from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API()

def conectar_y_preparar_hoja():
    # Añadimos reintento simple en la conexión inicial
    for intento in range(3):
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_JSON, scope)
            client = gspread.authorize(creds)
            sheet = client.open(SPREADSHEET_NAME).get_worksheet(0)
            return sheet
        except Exception as e:
            print(f"⚠️ Intento {intento+1} fallido en Sheets: {e}")
            time.sleep(2)
    return None

@app.get("/")
def home():
    return {"status": "online", "msg": "Recolector Barajas V4 - Lógica de Estado Activa (Blindado)"}

@app.get("/recolectar")
def recolectar():
    try:
        sheet = conectar_y_preparar_hoja()
        if not sheet:
            return JSONResponse({"status": "error", "msg": "No se pudo conectar a Google Sheets"}, status_code=500)

        # MEDIDA DE PROTECCIÓN 1: Lectura optimizada
        try:
            total_filas = len(sheet.col_values(1))
            fila_inicio = max(1, total_filas - 1500)
            data_reciente = sheet.get(f"A{fila_inicio}:N{total_filas+1}")
            firmas_existentes = {fila[1] for fila in data_reciente if len(fila) > 1}
        except Exception as e:
            print(f"⚠️ Error leyendo datos recientes, usando set vacío: {e}")
            firmas_existentes = set()

        # OBTENER DATOS DE RADAR
        try:
            flights = fr_api.get_flights(bounds=fr_api.get_bounds_by_point(40.48, -3.56, 15000))
        except Exception as e:
            return JSONResponse({"status": "error", "msg": f"Error FR24: {e}"}, status_code=500)

        nuevos_registros = []
        ahora = datetime.now(ZONA_HORARIA)

        for f in flights:
            try:
                if f.on_ground == 1: continue 
                
                detalles = fr_api.get_flight_details(f)
                if not detalles or 'status' not in detalles: continue
                
                estado = detalles['status']['generic']['status']['text']
                tipo_mov = None
                if estado == "Landed": tipo_mov = "LLEGADA"
                elif estado == "Take-off": tipo_mov = "SALIDA"
                
                if tipo_mov:
                    vuelo_id = f.number if f.number != "N/A" else f.callsign
                    ts_real = detalles['time']['real']['at'] or detalles['time']['real']['departure']
                    
                    if ts_real:
                        firma_nueva = f"{vuelo_id}_{tipo_mov}_{ts_real}"
                        if firma_nueva not in firmas_existentes:
                            d = detalles
                            dt_real = datetime.fromtimestamp(ts_real, ZONA_HORARIA)
                            
                            apt_key = 'origin' if tipo_mov == "SALIDA" else 'destination'
                            ts_key = 'departure' if tipo_mov == "SALIDA" else 'arrival'
                            
                            ciudad = d['airport'][apt_key]['city'] if d['airport'][apt_key] else "N/A"
                            pais = d['airport'][apt_key]['code']['iata'] if d['airport'][apt_key] else "N/A"
                            aerolinea = d['airline']['name'] if d['airline'] else "N/A"
                            
                            # MEJORA 2: Normalización de Terminal
                            terminal = d['airport']['origin']['info']['terminal'] or "N/A"
                            if terminal != "N/A" and not terminal.startswith("T"):
                                terminal = f"T{terminal}"
                            
                            # MEJORA 1: Filtro de retraso disparatado
                            diff_minutos = int((ts_real - d['time']['scheduled'][ts_key]) / 60)
                            if abs(diff_minutos) > 1440: # Umbral de 24 horas
                                diff_minutos = 0
                                
                            categoria = "COMERCIAL" if d['identification']['number']['default'] else "PRIVADO/CHARTER"
                            
                            nuevos_registros.append([
                                ahora.strftime('%Y-%m-%d %H:%M:%S'),
                                vuelo_id, tipo_mov,
                                d['airport'][apt_key]['code']['iata'],
                                ciudad, pais, aerolinea, terminal,
                                dt_real.strftime('%Y-%m-%d %H:%M:%S'),
                                d['aircraft']['model']['text'],
                                d['aircraft']['registration'],
                                diff_minutos, categoria, ts_real
                            ])
                            firmas_existentes.add(firma_nueva)
            except:
                continue

        if nuevos_registros:
            # MEDIDA DE PROTECCIÓN 2: Reintento exponencial en escritura
            for intento in range(3):
                try:
                    sheet.append_rows(nuevos_registros)
                    return {"status": "success", "añadidos": len(nuevos_registros)}
                except Exception as e:
                    if intento < 2:
                        print(f"⚠️ Error escribiendo, reintentando en {2**(intento+1)}s...")
                        time.sleep(2**(intento+1))
                    else:
                        return JSONResponse({"status": "error", "msg": f"Fallo tras reintentos: {e}"}, status_code=500)
        
        return {"status": "success", "añadidos": 0}

    except Exception as e:
        return JSONResponse({"status": "error", "msg": str(e)}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)















