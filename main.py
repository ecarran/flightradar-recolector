import os
import time
import pytz
import gspread
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from oauth2client.service_account import ServiceAccountCredentials
from flightradar24 import Api # Importación verificada según tu archivo [cite: 77]
from datetime import datetime

# ==================================
# CONFIGURACIÓN
# ==================================
IATA_CODE = "MAD"
ZONA_HORARIA = pytz.timezone("Europe/Madrid")
GOOGLE_JSON = "service_account.json" 
SPREADSHEET_NAME = "Barajas_Master_Data"

ENCABEZADOS = [
    "Fecha_Carga", "Vuelo", "Tipo", "IATA", "Ciudad", "Pais", 
    "Aerolinea", "Terminal", "Hora_Real", "Modelo_Avion", 
    "Matricula", "Diferencia", "Categoria", "TS_Firma"
]

app = FastAPI()
fr_api = Api() # Clase correcta según tu investigación [cite: 95]

def conectar_hoja():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_JSON, scope)
        client = gspread.authorize(creds)
        return client.open(SPREADSHEET_NAME).get_worksheet(0)
    except: return None

@app.get("/")
def home(): return {"msg": "Recolector Barajas V10 - Operativo"}

@app.get("/recolectar")
def recolectar():
    sheet = conectar_hoja()
    if not sheet: return JSONResponse({"error": "Error Sheets"}, status_code=500)

    # Cargar firmas para evitar duplicados
    data_actual = sheet.get_all_values()
    firmas_existentes = {f"{r[1]}_{r[13]}" for r in data_actual[1:] if len(r) > 13}
    
    try:
        # Usamos get_flights que es el método real del paquete 
        # Para Barajas, lo más efectivo es rastrear las aerolíneas principales
        aerolineas_principales = ["IBE", "VLG", "AEA", "RYR"]
        nuevos_registros = []
        ahora = datetime.now(ZONA_HORARIA)

        for airline in aerolineas_principales:
            vuelos = fr_api.get_flights(airline) # Método verificado 
            
            # El paquete devuelve un diccionario con los datos del vuelo
            for fid, data in vuelos.items():
                if isinstance(data, list) and len(data) > 1:
                    # Filtrar solo si el destino u origen es MAD
                    # La estructura interna del feed varía, pero aquí buscamos el código MAD
                    if IATA_CODE not in str(data): continue
                    
                    vuelo_no = data[13] if len(data) > 13 else fid
                    ts_real = data[10] # Timestamp de la última actualización
                    
                    firma = f"{vuelo_no}_{ts_real}"
                    if firma not in firmas_existentes:
                        nuevos_registros.append([
                            ahora.strftime('%Y-%m-%d %H:%M:%S'),
                            vuelo_no,
                            "N/A", # Esta librería básica no distingue Tipo fácilmente
                            IATA_CODE,
                            "Madrid", "Spain", airline, "N/A",
                            datetime.fromtimestamp(ts_real, ZONA_HORARIA).strftime('%Y-%m-%d %H:%M:%S'),
                            data[8], # Modelo
                            data[9], # Matrícula
                            0, "COMERCIAL", ts_real
                        ])
                        firmas_existentes.add(firma)

        if nuevos_registros:
            sheet.append_rows(nuevos_registros)
            return {"status": "success", "añadidos": len(nuevos_registros)}
        return {"status": "success", "añadidos": 0}

    except Exception as e:
        return JSONResponse({"status": "error", "msg": str(e)}, status_code=500)



