import os
import pandas as pd
import numpy as np
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SECRET_KEY = os.environ["SUPABASE_SECRET_KEY"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
GOOGLE_SHEET_GID = os.environ.get("GOOGLE_SHEET_GID", "0")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

# Leer Google Sheet
url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&gid={GOOGLE_SHEET_GID}"
df = pd.read_csv(url)

# Normalizar columnas
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)

print("Filas leídas desde Google Sheet:", len(df))
print("Columnas del sheet:", list(df.columns))
print("Últimos ID del sheet:")
print(df["id_actualizacion"].tail(10))

# Limpiar nulos
df = df.replace({np.nan: None})
df = df.where(pd.notnull(df), None)

# Formatear fecha
if "fecha" in df.columns:
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["fecha"] = df["fecha"].dt.strftime("%Y-%m-%d")
    df["fecha"] = df["fecha"].where(df["fecha"].notna(), None)

# Buscar último id cargado
resp = (
    supabase.schema("bronze")
    .table("avance_raw")
    .select("id_actualizacion")
    .order("id_actualizacion", desc=True)
    .limit(1)
    .execute()
)

ultimo_id = None
if resp.data:
    ultimo_id = resp.data[0]["id_actualizacion"]

# Filtrar incremental
if ultimo_id is not None:
    df = df[df["id_actualizacion"] > ultimo_id]

if df.empty:
    print("No hay registros nuevos.")
    raise SystemExit(0)

records = []

for _, row in df.iterrows():
    data = row.to_dict()

    records.append({
        "id_actualizacion": data.get("id_actualizacion"),
        "semana": int(data["semana"]) if data.get("semana") is not None else None,
        "anio": int(data["anio"]) if data.get("anio") is not None else None,
        "semanas": data.get("semanas"),
        "mes": data.get("mes"),
        "fecha": data.get("fecha"),
        "cuadrilla": data.get("cuadrilla"),
        "zona": data.get("zona"),
        "nivel": int(data["nivel"]) if data.get("nivel") is not None else None,
        "tipo_labor": data.get("tipo_labor"),
        "labor": data.get("labor"),
        "guardia": data.get("guardia"),
        "turno": data.get("turno"),
        "jefe_guardia": data.get("jefe_guardia"),
        "sup_tecnico": data.get("sup_tecnico"),
        "supervisor_tecnico": data.get("supervisor_tecnico"),
        "op_jumbo": data.get("op_jumbo"),
        "operador_jumbo": data.get("operador_jumbo"),
        "avance": float(data["avance"]) if data.get("avance") is not None else None,
        "ancho_proyectado": float(data["ancho_proyectado"]) if data.get("ancho_proyectado") is not None else None,
        "alto_proyectado": float(data["alto_proyectado"]) if data.get("alto_proyectado") is not None else None,
        "ancho_real": float(data["ancho_real"]) if data.get("ancho_real") is not None else None,
        "alto_real": float(data["alto_real"]) if data.get("alto_real") is not None else None,
        "tipo_voladura": data.get("tipo_voladura"),
        "volumen_aparente": float(data["volumen_aparente"]) if data.get("volumen_aparente") is not None else None,
        "volumen_medido": float(data["volumen_medido"]) if data.get("volumen_medido") is not None else None,
        "numero_taladros": int(data["numero_taladros"]) if data.get("numero_taladros") is not None else None,
        "longitud_perforacion": float(data["longitud_perforacion"]) if data.get("longitud_perforacion") is not None else None,
        "senatel_pulsar_kg": float(data["senatel_pulsar_kg"]) if data.get("senatel_pulsar_kg") is not None else None,
        "senatel_magnafrac_kg": float(data["senatel_magnafrac_kg"]) if data.get("senatel_magnafrac_kg") is not None else None,
        "anfo_kg": float(data["anfo_kg"]) if data.get("anfo_kg") is not None else None
    })

chunk_size = 300
for i in range(0, len(records), chunk_size):
    chunk = records[i:i+chunk_size]
    supabase.schema("bronze").table("avance_raw").insert(chunk).execute()

print(f"Carga incremental completada. Registros nuevos: {len(records)}")
