import os
import time
import pytz
import gspread
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# --- IMPORTACIÓN DE LA LIBRERÍA LOCAL ---
from FlightRadar24 import FlightRadar24API

# --- CONFIGURACIÓN ---
IATA_CODE = "MAD"
ZONA_HORARIA = pytz.timezone("Europe/Madrid")
GOOGLE_JSON = "service_account.json" 
SPREADSHEET_NAME = "Barajas_Master_Data"

app = FastAPI()
fr_api = FlightRadar24API()

def conectar_y_preparar_hoja():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_JSON, scope)
        client = gspread.authorize(creds)
        sheet = client.open(SPREADSHEET_NAME).get_worksheet(0)
        return sheet
    except Exception as e:
        print(f"⛔ Error en Sheets: {e}")
        return None

@app.get("/")
def home():
    return {"status": "online", "msg": "Recolector Barajas Optimizado - Operativo"}

@app.get("/ping")
def ping():
    return {"status": "alive", "timestamp": datetime.now(ZONA_HORARIA).isoformat()}
    
@app.get("/recolectar")
def recolectar():
    sheet = conectar_y_preparar_hoja()
    if not sheet:
        return JSONResponse({"error": "No se pudo conectar a Google Sheets"}, status_code=500)

    # --- LECTURA OPTIMIZADA ---
    total_filas = sheet.row_count
    inicio_lectura = max(1, total_filas - 400)
    data_reciente = sheet.get_values(f"A{inicio_lectura}:N{total_filas}")
    
    # CORRECCIÓN DE BUG: Normalización a string y limpieza de espacios
    # Usamos str().strip() para que la comparación sea idéntica aunque Google Sheets cambie el formato
    firmas_existentes = {
        f"{str(r[1]).strip()}_{str(r[13]).strip()}" 
        for r in data_reciente 
        if len(r) > 13 and str(r[1]).strip() != "Vuelo"
    }
    
    try:
        aeropuerto = fr_api.get_airport(code = IATA_CODE)
        bounds = fr_api.get_bounds_by_point(aeropuerto.latitude, aeropuerto.longitude, 50000)
        vuelos_radar = fr_api.get_flights(bounds = bounds)
        
        nuevos_registros = []
        ahora = datetime.now(ZONA_HORARIA)
        ahora_ts = ahora.timestamp()

        for v in vuelos_radar:
            # ALTITUD ASIMÉTRICA (Mantenida)
            es_mad_origen = v.origin_airport_iata == IATA_CODE
            es_mad_destino = v.destination_airport_iata == IATA_CODE

            if not (es_mad_origen or es_mad_destino):
                continue
            if es_mad_destino and v.altitude > 5000:
                continue
            if es_mad_origen and v.altitude > 10000:
                continue

            try:
                d = fr_api.get_flight_details(v)
                
                es_salida = d['airport']['origin']['code']['iata'] == IATA_CODE
                es_llegada = d['airport']['destination']['code']['iata'] == IATA_CODE
                
                if not (es_salida or es_llegada): continue
                
                apt_key = 'destination' if es_salida else 'origin'
                ts_key = 'departure' if es_salida else 'arrival'
                ts_real = d['time']['real'].get(ts_key)
                
                if ts_real and (ahora_ts - ts_real) < 5400:
                    vuelo_id = d['identification']['number']['default'] or d['aircraft']['registration']
                    categoria = "COMERCIAL" if d['identification']['number']['default'] else "PRIVADO/CHARTER"
                    
                    # CORRECCIÓN DE BUG: Generación de firma normalizada
                    firma = f"{str(vuelo_id).strip()}_{str(ts_real).strip()}"
                    
                    if firma not in firmas_existentes:
                        ciudad = d['airport'][apt_key]['position']['region']['city']
                        pais = d['airport'][apt_key]['position']['country']['name']
                        aerolinea = d['airline']['name'] if d['airline'] else "Privado"
                        terminal = d['airport']['origin' if es_salida else 'destination']['info']['terminal'] or "N/A"
                        
                        diff_minutos = int((ts_real - d['time']['scheduled'][ts_key]) / 60)
                        dt_real = datetime.fromtimestamp(ts_real, ZONA_HORARIA)
                        
                        nuevos_registros.append([
                            ahora.strftime('%Y-%m-%d %H:%M:%S'),
                            vuelo_id,
                            "SALIDA" if es_salida else "LLEGADA",
                            d['airport'][apt_key]['code']['iata'],
                            ciudad, pais, aerolinea, terminal,
                            dt_real.strftime('%Y-%m-%d %H:%M:%S'),
                            d['aircraft']['model']['text'],
                            d['aircraft']['registration'],
                            diff_minutos, categoria, ts_real
                        ])
                        firmas_existentes.add(firma)
                        time.sleep(0.05)
            except:
                continue

        if nuevos_registros:
            sheet.append_rows(nuevos_registros)
            return {"status": "success", "añadidos": len(nuevos_registros)}
        
        return {"status": "success", "añadidos": 0}

    except Exception as e:
        return JSONResponse({"status": "error", "msg": str(e)}, status_code=500)










