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
    return {"status": "online", "msg": "Recolector Barajas V3 - Firma de Bloque Activa"}

@app.get("/recolectar")
def recolectar():
    sheet = conectar_y_preparar_hoja()
    if not sheet:
        return JSONResponse({"error": "No se pudo conectar a Google Sheets"}, status_code=500)

    # --- 1. CAPTURA DE FIRMAS EXISTENTES (ESTABILIZADA) ---
    todos_los_datos = sheet.get_all_values()
    # Miramos los últimos 600 para cubrir horas punta de gran tráfico
    data_reciente = todos_los_datos[-600:] if len(todos_los_datos) > 600 else todos_los_datos
    
    firmas_existentes = set()
    for r in data_reciente:
        if len(r) > 13:
            # Creamos firma estable: Vuelo + Fecha (sin hora) + Tipo
            # La fecha está en r[8] (Hora_Real), extraemos solo YYYY-MM-DD
            fecha_vuelo_existente = r[8].split(' ')[0] if ' ' in r[8] else r[8]
            id_unico = f"{str(r[1]).strip()}_{fecha_vuelo_existente}_{str(r[2]).strip()}"
            firmas_existentes.add(id_unico)

    try:
        aeropuerto = fr_api.get_airport(code = IATA_CODE)
        bounds = fr_api.get_bounds_by_point(aeropuerto.latitude, aeropuerto.longitude, 50000)
        vuelos_radar = fr_api.get_flights(bounds = bounds)
        
        nuevos_registros = []
        ahora = datetime.now(ZONA_HORARIA)
        ahora_ts = ahora.timestamp()

        for v in vuelos_radar:
            es_mad_origen = v.origin_airport_iata == IATA_CODE
            es_mad_destino = v.destination_airport_iata == IATA_CODE

            if not (es_mad_origen or es_mad_destino): continue
            
            # Filtros de altitud (Salidas 10k, Llegadas 5k)
            if es_mad_destino and v.altitude > 5000: continue
            if es_mad_origen and v.altitude > 10000: continue

            try:
                d = fr_api.get_flight_details(v)
                es_salida = d['airport']['origin']['code']['iata'] == IATA_CODE
                es_llegada = d['airport']['destination']['code']['iata'] == IATA_CODE
                
                if not (es_salida or es_llegada): continue
                
                apt_key = 'destination' if es_salida else 'origin'
                ts_key = 'departure' if es_salida else 'arrival'
                ts_real = d['time']['real'].get(ts_key)
                
                # Solo procesar si hay timestamp REAL (el avión ya aterrizó o despegó)
                if ts_real and (ahora_ts - ts_real) < 5400:
                    vuelo_id = str(d['identification']['number']['default'] or d['aircraft']['registration']).strip()
                    tipo_mov = "SALIDA" if es_salida else "LLEGADA"
                    dt_real = datetime.fromtimestamp(ts_real, ZONA_HORARIA)
                    fecha_vuelo_str = dt_real.strftime('%Y-%m-%d')
                    
                    # --- COMPROBACIÓN DE FIRMA DE BLOQUE ---
                    firma_nueva = f"{vuelo_id}_{fecha_vuelo_str}_{tipo_mov}"
                    
                    if firma_nueva not in firmas_existentes:
                        ciudad = d['airport'][apt_key]['position']['region']['city']
                        pais = d['airport'][apt_key]['position']['country']['name']
                        aerolinea = d['airline']['name'] if d['airline'] else "Privado"
                        terminal = d['airport']['origin' if es_salida else 'destination']['info']['terminal'] or "N/A"
                        
                        diff_minutos = int((ts_real - d['time']['scheduled'][ts_key]) / 60)
                        categoria = "COMERCIAL" if d['identification']['number']['default'] else "PRIVADO/CHARTER"
                        
                        nuevos_registros.append([
                            ahora.strftime('%Y-%m-%d %H:%M:%S'),
                            vuelo_id,
                            tipo_mov,
                            d['airport'][apt_key]['code']['iata'],
                            ciudad, pais, aerolinea, terminal,
                            dt_real.strftime('%Y-%m-%d %H:%M:%S'),
                            d['aircraft']['model']['text'],
                            d['aircraft']['registration'],
                            diff_minutos, categoria, ts_real
                        ])
                        firmas_existentes.add(firma_nueva)
                        time.sleep(0.05)
            except:
                continue

        if nuevos_registros:
            sheet.append_rows(nuevos_registros)
            return {"status": "success", "añadidos": len(nuevos_registros)}
        
        return {"status": "success", "añadidos": 0}

    except Exception as e:
        return JSONResponse({"status": "error", "msg": str(e)}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)













