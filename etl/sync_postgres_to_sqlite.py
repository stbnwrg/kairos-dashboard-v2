import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine

TABLES = [
    "fact_ventas",
    "fact_gastos",
    "fact_items",
    "dim_secciones",
    "dim_calendario",
]

def main():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("Falta DATABASE_URL en variables de entorno.")

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sqlite_path = os.path.join(project_root, "database", "kairos.db")
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)

    print("Conectando a Postgres...")
    engine = create_engine(database_url)

    print(f"Creando SQLite en: {sqlite_path}")
    conn = sqlite3.connect(sqlite_path)

    for t in TABLES:
        print(f"-> Exportando {t} ...")
        df = pd.read_sql(f"SELECT * FROM {t}", engine)
        df.to_sql(t, conn, if_exists="replace", index=False)

    conn.close()
    print("✅ Sync terminado. Abre database/kairos.db en VS Code.")

if __name__ == "__main__":
    main()