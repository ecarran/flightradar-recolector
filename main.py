import os
import time
import pytz
import gspread
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from oauth2client.service_account import ServiceAccountCredentials
from FlightRadar24 import FlightRadar24API
from datetime import datetime

# --- CONFIGURACIÓN ---
IATA_CODE = "MAD"
ZONA_HORARIA = pytz.timezone("Europe/Madrid")
# En Render, el archivo se llama exactamente así si lo subes como Secret File
GOOGLE_JSON = "service_account.json" 
SPREADSHEET_NAME = "Barajas_Master_Data"

# Estructura de columnas optimizada para Power BI
ENCABEZADOS = [
    "Fecha_Carga", "Vuelo", "Tipo", "IATA", "Ciudad", "Pais", 
    "Aerolinea", "Terminal", "Hora_Real", "Modelo_Avion", 
    "Matricula", "Diferencia", "Categoria", "TS_Firma"
]

app = FastAPI()
fr_api = FlightRadar24API() # Usamos la librería potente

def conectar_y_preparar_hoja():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        # Buscamos el archivo en el directorio raíz de Render
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_JSON, scope)
        client = gspread.authorize(creds)
        sheet = client.open(SPREADSHEET_NAME).get_worksheet(0)
        return sheet
    except Exception as e:
        print(f"⛔ Error en Sheets: {e}")
        return None

@app.get("/")
def home():
    return {"status": "online", "msg": "Recolector Barajas Pro V1 - Operativo"}

@app.get("/recolectar")
def recolectar():
    sheet = conectar_y_preparar_hoja()
    if not sheet:
        return JSONResponse({"error": "No se pudo conectar a Google Sheets"}, status_code=500)

    # Carga de firmas existentes para evitar duplicados
    data_actual = sheet.get_all_values()
    firmas_existentes = {f"{r[1]}_{r[13]}" for r in data_actual[1:] if len(r) > 13}
    
    try:
        # Lógica local: Obtenemos coordenadas de Barajas y definimos el radio
        aeropuerto = fr_api.get_airport(code = IATA_CODE)
        bounds = fr_api.get_bounds_by_point(aeropuerto.latitude, aeropuerto.longitude, 50000)
        vuelos_radar = fr_api.get_flights(bounds = bounds)
        
        nuevos_registros = []
        ahora = datetime.now(ZONA_HORARIA)
        ahora_ts = ahora.timestamp()

        for v in vuelos_radar:
            # Filtro de altitud para aeronaves en aproximación o despegue
            if v.altitude > 6000 and v.ground_speed > 250:
                continue 

            try:
                d = fr_api.get_flight_details(v)
                
                # Identificar si toca Madrid
                es_salida = d['airport']['origin']['code']['iata'] == IATA_CODE
                es_llegada = d['airport']['destination']['code']['iata'] == IATA_CODE
                if not (es_salida or es_llegada): continue
                
                apt_key = 'destination' if es_salida else 'origin'
                ts_key = 'departure' if es_salida else 'arrival'
                ts_real = d['time']['real'].get(ts_key)
                
                # Ventana de tiempo para capturar datos recientes
                if ts_real and (ahora_ts - ts_real) < 5400:
                    vuelo_id = d['identification']['number']['default'] or d['aircraft']['registration']
                    categoria = "COMERCIAL" if d['identification']['number']['default'] else "PRIVADO/CHARTER"
                    
                    firma = f"{vuelo_id}_{ts_real}"
                    
                    if firma not in firmas_existentes:
                        # Extracción enriquecida para Power BI
                        ciudad = d['airport'][apt_key]['position']['region']['city']
                        pais = d['airport'][apt_key]['position']['country']['name']
                        aerolinea = d['airline']['name'] if d['airline'] else "Privado"
                        terminal = d['airport']['origin' if es_salida else 'destination']['info']['terminal'] or "N/A"
                        
                        # Cálculo de puntualidad
                        diff_minutos = int((ts_real - d['time']['scheduled'][ts_key]) / 60)
                        dt_real = datetime.fromtimestamp(ts_real, ZONA_HORARIA)
                        
                        nuevos_registros.append([
                            ahora.strftime('%Y-%m-%d %H:%M:%S'),
                            vuelo_id,
                            "SALIDA" if es_salida else "LLEGADA",
                            d['airport'][apt_key]['code']['iata'],
                            ciudad,
                            pais,
                            aerolinea,
                            terminal,
                            dt_real.strftime('%Y-%m-%d %H:%M:%S'),
                            d['aircraft']['model']['text'],
                            d['aircraft']['registration'],
                            diff_minutos,
                            categoria,
                            ts_real
                        ])
                        firmas_existentes.add(firma)
            except:
                continue

        if nuevos_registros:
            sheet.append_rows(nuevos_registros)
            return {"status": "success", "añadidos": len(nuevos_registros)}
        
        return {"status": "success", "añadidos": 0}

    except Exception as e:
        return JSONResponse({"status": "error", "msg": str(e)}, status_code=500)




