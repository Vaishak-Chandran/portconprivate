from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from utils.load_data import load_port_data
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd

app = FastAPI()

# === Database Configuration ===
DB_CONFIG = {
    "dbname": "sing",
    "user": "portcon",
    "password": "portuser",
    "host": "localhost",
    "port": "5432"
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

# === Dwelltime Endpoint ===
@app.get("/metrics/dwelltime")
def get_dwelltime():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                MAX(departure_date) AS latest_date,
                AVG(avg_dwell_hours) AS avg_dwell,
                MIN(min_dwell_hours) AS min_dwell,
                MAX(max_dwell_hours) AS max_dwell
            FROM daily_dwell_agg
        """)
        row = cur.fetchone()
        conn.close()
        return {
            "date": str(row["latest_date"]),
            "avg_dwell": float(row["avg_dwell"]),
            "min_dwell": float(row["min_dwell"]),
            "max_dwell": float(row["max_dwell"])
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# === Congestion History Endpoint ===
@app.get("/congestion/history")
def get_congestion_history(port: str = Query(...)):
    try:
        df = load_port_data(port)
        if "timestamp" not in df.columns:
            raise Exception("CSV missing 'timestamp' column")
        if "MMSI" not in df.columns:
            raise Exception("CSV missing 'MMSI' column")

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df["date"] = df["timestamp"].dt.date

        daily = df.groupby("date")["MMSI"].nunique().reset_index(name="congestion")

        return [
            {"date": str(row["date"]), "congestion": int(row["congestion"])}
            for _, row in daily.iterrows()
        ]
    except Exception as e:
        print("error:", e)
        return JSONResponse(status_code=500, content={"error": str(e)})

# === CORS Middleware ===
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
