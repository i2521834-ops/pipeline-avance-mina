import os
from pathlib import Path
import pandas as pd
from supabase import create_client
from datetime import datetime

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SECRET_KEY = os.environ["SUPABASE_SECRET_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

response = (
    supabase
    .schema("gold")
    .table("kpi_avance_mina")
    .select("*")
    .execute()
)

data = response.data
df = pd.DataFrame(data)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
Path("backups").mkdir(exist_ok=True)

filename = f"backups/gold_kpi_avance_mina_{timestamp}.csv"
df.to_csv(filename, index=False, encoding="utf-8-sig")

print(f"Backup generado: {filename}")
print(f"Filas exportadas: {len(df)}")
