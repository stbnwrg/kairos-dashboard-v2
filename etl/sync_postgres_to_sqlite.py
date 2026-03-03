import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, inspect


def main():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("Falta DATABASE_URL en variables de entorno.")

    # Paths
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sqlite_path = os.path.join(project_root, "database", "kairos.db")
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)

    print("Conectando a Postgres...")
    engine = create_engine(database_url)

    # Detectar automáticamente todas las tablas
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    if not tables:
        print("⚠️ No se encontraron tablas en Postgres.")
        return

    print(f"Tablas detectadas: {tables}")

    # Conectar SQLite
    print(f"Creando/Actualizando SQLite en: {sqlite_path}")
    conn = sqlite3.connect(sqlite_path)

    for t in tables:
        try:
            print(f"-> Exportando {t} ...")
            df = pd.read_sql(f'SELECT * FROM "{t}"', engine)
            df.to_sql(t, conn, if_exists="replace", index=False)
        except Exception as e:
            print(f"⚠️ Error exportando {t}: {e}")

    conn.close()
    print("✅ Sync terminado correctamente.")
    print("Abre database/kairos.db en VS Code para revisar.")


if __name__ == "__main__":
    main()