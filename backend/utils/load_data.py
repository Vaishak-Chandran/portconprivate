import pandas as pd

def load_port_data(port_name):
    file_map = {
        "singapore": "./data/singapore.csv",
        "rotterdam": "./data/rotterdam.csv",
        "la": "./data/la.csv"
    }

    file_path = file_map.get(port_name.lower())
    if not file_path:
        raise Exception("Invalid port name")

    df = pd.read_csv(file_path)
 
    if 'time_utc' not in df.columns:
        raise Exception(f"CSV missing 'time_utc' column. Columns found: {df.columns.tolist()}")

    df.rename(columns={"time_utc": "timestamp"}, inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)

    return df