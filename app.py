# ==============================================================================
# 📦 Importaciones y Configuración Global
# ==============================================================================
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import io

# Utilitarios para conexión y manejo de tablas en Supabase
from utils.db_utils import create_table_from_dataframe, insert_dataframe

# Inicialización de la Aplicación
app = FastAPI(title="CSV → Supabase Data Cleaner API")

# ==============================================================================
# 🌍 Configuración CORS (actualizada)
# ==============================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://avance23front-nez9.vercel.app",  # 🌐 dominio del frontend (Vercel)
        "http://localhost:5173",  # ⚙️ desarrollo local (Vite)
        "http://localhost:8080",
        "http://127.0.0.1:8080",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Variables globales
DATAFRAME_CACHE: pd.DataFrame = None
TABLE_NAME: str = None
last_cleaned_df: pd.DataFrame = None
# ==============================================================================
# ⚙️ Funciones Auxiliares
# ==============================================================================

def clean_dataframe(df: pd.DataFrame):
    """Limpieza general del dataframe, incluyendo IDs duplicados o nulos."""
    # 1️⃣ Eliminar duplicados
    df = df.drop_duplicates()

    # 2️⃣ Regenerar columna 'id' si está duplicada o vacía
    if 'id' in df.columns:
        if df['id'].duplicated().any() or df['id'].isnull().any():
            print("⚠️ IDs duplicados detectados → regenerando IDs únicos.")
            df = df.reset_index(drop=True)
            df['id'] = range(1, len(df) + 1)
    else:
        df['id'] = range(1, len(df) + 1)

    # 3️⃣ Imputar valores nulos
    df = df.fillna(df.select_dtypes(include=['number']).mean())
    df = df.fillna("Sin valor")

    return df


# ==============================================================================
# ⚡ Rutas de la API
# ==============================================================================

@app.get("/")
def root():
    return {"message": "✅ API activa. Usa /upload_csv para subir un archivo CSV."}


# ------------------------------------------------------------------------------
# 🔹 Subida y carga de CSV
# ------------------------------------------------------------------------------
@app.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...)):
    global DATAFRAME_CACHE, TABLE_NAME

    try:
        # Leer CSV
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        table_name = file.filename.split(".")[0].lower()

        # Limpieza inicial de IDs duplicados
        df = clean_dataframe(df)

        # Crear tabla e insertar datos en Supabase
        create_table_from_dataframe(table_name, df)
        insert_dataframe(table_name, df)

        # Guardar en caché
        DATAFRAME_CACHE = df
        TABLE_NAME = table_name

        return {
            "message": f"✅ {file.filename} cargado correctamente y limpiado.",
            "table_name": table_name,
            "columns": df.columns.tolist(),
            "rows": len(df),
        }

    except Exception as e:
        return {"error": f"❌ Error al procesar el CSV: {str(e)}"}


# ------------------------------------------------------------------------------
# 🔹 Analizar Datos
# ------------------------------------------------------------------------------
@app.get("/analyze_data")
def analyze_data():
    global DATAFRAME_CACHE

    if DATAFRAME_CACHE is None:
        return {"error": "❌ No hay dataset cargado. Sube un CSV primero."}

    df = DATAFRAME_CACHE
    analysis = []

    for col in df.columns:
        nulls = df[col].isna().sum()
        null_percentage = (nulls / len(df)) * 100 if len(df) > 0 else 0
        unique_vals = df[col].nunique()
        dtype = str(df[col].dtype)

        if null_percentage > 30:
            status = "error"
        elif null_percentage > 5:
            status = "warning"
        else:
            status = "clean"

        analysis.append({
            "name": col,
            "type": dtype,
            "nulls": int(nulls),
            "nullPercentage": round(null_percentage, 2),
            "unique": int(unique_vals),
            "status": status,
        })

    total_nulls = int(df.isna().sum().sum())
    total_duplicates = int(df.duplicated().sum())

    issues = [
        {
            "type": "Valores nulos",
            "count": total_nulls,
            "severity": "high" if total_nulls > 0 else "low",
            "affectedColumns": df.columns[df.isna().any()].tolist(),
        },
        {
            "type": "Duplicados",
            "count": total_duplicates,
            "severity": "medium" if total_duplicates > 0 else "low",
            "affectedColumns": df.columns.tolist(),
        },
    ]

    return {
        "table_name": TABLE_NAME,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": analysis,
        "issues": issues,
    }


# ------------------------------------------------------------------------------
# 🔹 Limpieza de Datos (por acción)
# ------------------------------------------------------------------------------
@app.post("/clean_data")
async def clean_data(payload: dict):
    global DATAFRAME_CACHE, TABLE_NAME, last_cleaned_df

    if DATAFRAME_CACHE is None:
        return {"error": "❌ No hay dataset cargado."}

    action = payload.get("action", "")
    df = DATAFRAME_CACHE.copy()

    try:
        if action == "clean_all":
            df = clean_dataframe(df)

        elif action.startswith("clean_"):
            col = action.replace("clean_", "")
            if col not in df.columns:
                return {"error": f"Columna '{col}' no encontrada."}

            if df[col].dtype == "object":
                df[col] = df[col].fillna("Desconocido")
            elif df[col].dtype in ["int64", "float64"]:
                df[col] = df[col].fillna(df[col].mean())

        else:
            return {"error": "Acción de limpieza no reconocida."}

        # Actualizar cache y base de datos
        DATAFRAME_CACHE = df
        last_cleaned_df = df
        insert_dataframe(TABLE_NAME, df)

        return {
            "message": f"✅ Limpieza '{action}' completada correctamente.",
            "rows_after": len(df),
        }

    except Exception as e:
        return {"error": f"❌ Error durante la limpieza: {str(e)}"}


# ------------------------------------------------------------------------------
# 🔹 Descargar dataset limpio
# ------------------------------------------------------------------------------
@app.get("/get_cleaned_csv")
def get_cleaned_csv():
    global last_cleaned_df
    if last_cleaned_df is None:
        return JSONResponse({"error": "❌ No hay datos limpios aún"}, status_code=400)

    return last_cleaned_df.to_dict(orient="records")
