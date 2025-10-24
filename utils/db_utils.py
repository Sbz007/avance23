from supabase import create_client
import pandas as pd

SUPABASE_URL = "https://vtyyobpvyadddyyulylh.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZ0eXlvYnB2eWFkZGR5eXVseWxoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDIxMjgyMSwiZXhwIjoyMDc1Nzg4ODIxfQ.lBI_pSwY7gZ84AbmZpnW55mHFv86BaONF9Xenfj0g28"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def create_table_from_dataframe(table_name: str, df: pd.DataFrame):
    # Generar SQL dinámico desde el DataFrame
    columns = ", ".join([f'"{col}" text' for col in df.columns])
    query = f'CREATE TABLE IF NOT EXISTS "{table_name}" (id serial primary key, {columns});'

    try:
        response = supabase.rpc("execute_sql", {"query": query}).execute()
        print(f"✅ Tabla '{table_name}' creada o existente.")
        print(response)
    except Exception as e:
        print(f"❌ Error creando tabla '{table_name}':", e)

def insert_dataframe(table_name: str, df: pd.DataFrame):
    try:
        data = df.to_dict(orient="records")
        response = supabase.table(table_name).insert(data).execute()
        print(f"✅ {len(df)} filas insertadas en '{table_name}'.")
        print(response)
    except Exception as e:
        print(f"❌ Error insertando datos en '{table_name}':", e)
