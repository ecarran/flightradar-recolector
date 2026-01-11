import os
import time
import pytz
import gspread
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from oauth2client.service_account import ServiceAccountCredentials
from FlightRadar24.api import FlightRadar24API
from datetime import datetime, timedelta

# ==================================
# CONFIGURACIÓN
# ==================================
IATA_CODE = "MAD"
ZONA_HORARIA = pytz.timezone("Europe/Madrid")
# En Render, asegúrate de subir este archivo como 'Secret File'
GOOGLE_JSON = "service_account.json" 
SPREADSHEET_NAME = "Barajas_Master_Data"

ENCABEZADOS = [
    "Fecha_Carga", "Vuelo", "Tipo", "IATA", "Ciudad", "Pais", 
    "Aerolinea", "Terminal", "Hora_Real", "Modelo_Avion", 
    "Matricula", "Diferencia", "Categoria", "TS_Firma"
]

app = FastAPI()
fr_api = FlightRadar24API()

# ==================================
# UTILIDADES DE CONEXIÓN
# ==================================
def conectar_hoja():
    """Establece conexión con Google Sheets y verifica encabezados."""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_JSON, scope)
        client = gspread.authorize(creds)
        sheet = client.open(SPREADSHEET_NAME).get_worksheet(0)
        
        # Verificar encabezados en la primera ejecución
        primera_fila = sheet.row_values(1)
        if not primera_fila or "Terminal" not in primera_fila:
            sheet.insert_row(ENCABEZADOS, 1)
        return sheet
    except Exception as e:
        print(f"⛔ Error Sheets: {e}")
        return None

# ==================================
# ENDPOINTS
# ==================================
@app.get("/")
def home():
    return {"msg": "Recolector Barajas V9 (FlightRadar24 -> Google Sheets)"}

@app.get("/ping")
def ping_service():
    """Endpoint para mantener vivo el servicio o verificar estado."""
    now = datetime.now(ZONA_HORARIA).strftime("%Y-%m-%d %H:%M:%S")
    return JSONResponse(content={"status": "alive", "time": now}, status_code=200)

@app.get("/recolectar")
def recolectar():
    """Lógica principal de captura y guardado en Sheets."""
    sheet = conectar_hoja()
    if not sheet:
        return JSONResponse({"error": "No se pudo conectar a Google Sheets"}, status_code=500)

    # 1. Cargar firmas existentes para evitar duplicados
    data_actual = sheet.get_all_values()
    # Firma en columna N (índice 13)
    firmas_existentes = {f"{r[1]}_{r[13]}" for r in data_actual[1:] if len(r) > 13}
    
    try:
        aeropuerto = fr_api.get_airport(code = IATA_CODE)
        bounds = fr_api.get_bounds_by_point(aeropuerto.latitude, aeropuerto.longitude, 50000)
        vuelos_radar = fr_api.get_flights(bounds = bounds)
        
        nuevos_registros = []
        ahora_ts = datetime.now(ZONA_HORARIA).timestamp()
        limite_ventana = ahora_ts - 5400 # 90 minutos

        for v in vuelos_radar:
            # Pre-filtro de eficiencia
            if v.altitude > 6000 and v.ground_speed > 250:
                continue 

            try:
                d = fr_api.get_flight_details(v)
                es_salida = d['airport']['origin']['code']['iata'] == IATA_CODE
                es_llegada = d['airport']['destination']['code']['iata'] == IATA_CODE
                
                if not (es_salida or es_llegada): continue
                
                apt_key = 'destination' if es_salida else 'origin'
                ts_key = 'departure' if es_salida else 'arrival'
                ts_real = d['time']['real'].get(ts_key)
                
                if ts_real and ts_real >= limite_ventana:
                    vuelo_id = d['identification']['number']['default'] or d['aircraft']['registration']
                    categoria = "COMERCIAL" if d['identification']['number']['default'] else "PRIVADO/CHARTER"
                    
                    firma = f"{vuelo_id}_{ts_real}"
                    
                    if firma not in firmas_existentes:
                        # Datos enriquecidos para Power BI
                        ciudad = d['airport'][apt_key]['position']['region']['city']
                        pais = d['airport'][apt_key]['position']['country']['name']
                        aerolinea = d['airline']['name'] if d['airline'] else "Privado"
                        terminal = d['airport']['origin' if es_salida else 'destination']['info']['terminal'] or "N/A"
                        diff_minutos = int((ts_real - d['time']['scheduled'][ts_key]) / 60)
                        dt_real = datetime.fromtimestamp(ts_real, ZONA_HORARIA)
                        
                        nuevos_registros.append([
                            datetime.now(ZONA_HORARIA).strftime('%Y-%m-%d %H:%M:%S'),
                            vuelo_id,
                            "SALIDA" if es_salida else "LLEGADA",
                            d['airport'][apt_key]['code']['iata'],
                            ciudad, pais, aerolinea, terminal,
                            dt_real.strftime('%Y-%m-%d %H:%M:%S'),
                            d['aircraft']['model']['text'],
                            d['aircraft']['registration'],
                            diff_minutos,
                            categoria,
                            ts_real
                        ])
                        firmas_existentes.add(firma)
                        time.sleep(0.4) 
            except: continue

        if nuevos_registros:
            sheet.append_rows(nuevos_registros)
            return {"status": "success", "nuevos": len(nuevos_registros)}
        
        return {"status": "success", "nuevos": 0}

    except Exception as e:

        return JSONResponse({"status": "error", "msg": str(e)}, status_code=500)
