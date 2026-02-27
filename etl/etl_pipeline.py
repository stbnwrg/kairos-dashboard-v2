import os
import pandas as pd
from sqlalchemy import create_engine


# =====================================================
# PATHS
# =====================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

# Carpeta uploads (creada dinámicamente desde app)
UPLOADS_DIR = os.path.join(PROJECT_ROOT, "uploads")

RUTA_GASTOS = os.path.join(UPLOADS_DIR, "gastos.xls")
RUTA_VENTAS = os.path.join(UPLOADS_DIR, "ventas.xlsx")
RUTA_COSTO_UNITARIO = os.path.join(UPLOADS_DIR, "costo_unitario.xlsx")



# =====================================================
# VALIDA RUTAS
# =====================================================


#if not os.path.exists(RUTA_GASTOS) or not os.path.exists(RUTA_VENTAS):
#    print("No hay archivos cargados aún.")
#    exit()
# =====================================================
# VALIDACIÓN ARCHIVOS
# =====================================================

#if not os.path.exists(RUTA_GASTOS) or not os.path.exists(RUTA_VENTAS):
#    print("No hay archivos cargados aún.")
#    exit()


# =====================================================
# UTILIDADES
# =====================================================

def limpiar_columnas(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("á", "a")
        .str.replace("é", "e")
        .str.replace("í", "i")
        .str.replace("ó", "o")
        .str.replace("ú", "u")
    )
    return df


def limpiar_fecha(col):
    return pd.to_datetime(
        col.astype(str).str[-10:],
        dayfirst=True,
        errors="coerce"
    )

# =====================================================
# GASTOS
# =====================================================
def procesar_gastos():

    FECHA_INICIO_OPERACION = pd.Timestamp("2025-10-01")

    df = pd.read_excel(RUTA_GASTOS, sheet_name="Gastos", skiprows=1)
    df = limpiar_columnas(df)

    df["fecha"] = limpiar_fecha(df["fecha"])
    df["total"] = pd.to_numeric(df["total"], errors="coerce")

    df = df.dropna(subset=["fecha", "total"])

    df["tipo"] = df["tipo"].replace("PÏZZA", "PIZZA")

    # -----------------------
    # GRUPO 1 (según Excel)
    # -----------------------
    def grupo_1(row):
        if row["tipo"] in [
            "COMISIONES VENTAS",
            "INSUMO",
            "IMPLEMENTACIÓN",
            "SERVICIOS",
            "SOFTWARE",
            "REMUNERACIONES",
            "ARRIENDO",
            "LUZ",
            "AGUA",
            "GASTOS COMUNES"
        ]:
            return "OTROS"
        return row["tipo"]

    df["grupo_1"] = df.apply(grupo_1, axis=1)

    # -----------------------
    # GRUPO 2
    # -----------------------
    def grupo_2(row):
        if row["tipo"] == "COMISIONES VENTAS":
            return "ADMINISTRATIVOS"
        elif row["tipo"] == "INSUMO":
            return "OTROS INSUMOS"
        elif row["tipo"] in ["SERVICIOS", "SOFTWARE", "REMUNERACIONES"]:
            return "ADMINISTRATIVOS"
        return row["tipo"]

    df["grupo_2"] = df.apply(grupo_2, axis=1)

    # -----------------------
    # CLASIFICACIÓN FINANCIERA (NUEVA)
    # -----------------------
    def clasificacion_financiera(row):
        # 1) CAPEX (inversión real)
        CAPEX_PROVEEDORES = [
    "CHILENA DE CAFES SpA",
    "CONSTRUCTORA CELSA SPA",
    "FABRICA DE MUEBLES INTERKITT LIMITADA",
    "BOZZO S.A."
]

        if row["comentario"] in CAPEX_PROVEEDORES:
            return "CAPEX"

        # 2) PRE-OPERACIÓN (antes de abrir)
        if row["fecha"] < FECHA_INICIO_OPERACION:
            return "PRE_OPERACION"

        # 3) Variables operativas
        if row["tipo"] in [
            "COMISIONES VENTAS",
            "PIZZA",
            "INSUMO",
            "CAFÉ",
            "TÉ",
            "PASTELERÍA"
        ]:
            return "OPEX_VARIABLE"

        # 4) Fijos operativos
        return "OPEX_FIJO"

    df["clasificacion"] = df.apply(clasificacion_financiera, axis=1)

    # ✅ OJO: eliminamos fecha_2 porque dependía de clasificacion y ya no aplica.

    return df.reset_index(drop=True)

# =====================================================
# TRANSACCIONES
# =====================================================

def procesar_transacciones():

    df = pd.read_excel(RUTA_VENTAS, sheet_name="Transacciones", skiprows=1)
    df = limpiar_columnas(df)

    df["fecha_completado"] = limpiar_fecha(df["fecha_completado"])
    df.rename(columns={"fecha_completado": "fecha"}, inplace=True)

    df["total"] = pd.to_numeric(df["total"], errors="coerce")
    df = df.dropna(subset=["fecha", "total"])

    df["iva"] = (df["total"] * 0.19).round(0)
    df["total_sin_iva"] = df["total"] - df["iva"]

    return df.reset_index(drop=True)

# =====================================================
# ITEMS
# =====================================================

def procesar_items():

    df = pd.read_excel(RUTA_VENTAS, sheet_name="Items", skiprows=1)
    df = limpiar_columnas(df)

    df["fecha_completado"] = limpiar_fecha(df["fecha_completado"])
    df.rename(columns={"fecha_completado": "fecha"}, inplace=True)

    df["precio"] = pd.to_numeric(df["precio"], errors="coerce")
    df = df.dropna(subset=["fecha", "precio"])

    return df.reset_index(drop=True)

# =====================================================
# SECCIONES
# =====================================================

def procesar_secciones():

    df = pd.read_excel(RUTA_VENTAS, sheet_name="Secciones", skiprows=1)
    df = limpiar_columnas(df)

    df["total"] = pd.to_numeric(df["total"], errors="coerce")
    df = df.dropna(subset=["seccion", "total"])

    # -----------------------
    # GRUPO 2
    # -----------------------
    def grupo_2(seccion):

        mapping = {
            "Sándwiches": "SANDWICH",
            "🌟 Diferenciadores (Experiencia Café Kairós)": "CAFÉ",
            "Café": "CAFÉ",
            "Croissant Salados": "PASTELERÍA",
            "Pastelería": "PASTELERÍA",
            "Waffles": "PASTELERÍA",
            "Jugos naturales": "BEBIDAS FRIAS",
            "Bollería": "PASTELERÍA",
            "Batidos": "BEBIDAS FRIAS",
            "Bebidas frías y otras opciones": "BEBIDAS FRIAS",
            "Brunch": "OTROS",
            "Pizza de la casa": "PIZZA",
            "Helados": "HELADOS",
            "Productos Blackdrop Coffee": "CAFÉ",
            "Promociones día del profesor/a": "PROMOCIONES",
            "Promoción Lunes \"Café + Torta del día\"": "PROMOCIONES",
            "🌟 Diferenciadores (Bebidas Frías)": "CAFÉ",
            "Cajas dulce Kairós": "PASTELERÍA",
            "🌟 Latte Blackdrop (producto vegano)": "CAFÉ",
            "Momento Kairós - Fotografía": "OTROS",
            "Promociones Kairós": "PROMOCIONES"
        }

        return mapping.get(seccion, "TÉ")

    df["grupo_2"] = df["seccion"].apply(grupo_2)

    # -----------------------
    # GRUPO 1
    # -----------------------
    def grupo_1(row):
        if row["grupo_2"] in ["CAFÉ", "PASTELERÍA", "TÉ", "PIZZA"]:
            return row["grupo_2"]
        return "OTROS"

    df["grupo_1"] = df.apply(grupo_1, axis=1)

    return df.reset_index(drop=True)

# =====================================================
# PROCESAR COSTOS UNITARIOS
# =====================================================

def procesar_costos_unitarios(df_secciones_ref: pd.DataFrame | None = None):

    if not os.path.exists(RUTA_COSTO_UNITARIO):
        return pd.DataFrame()

    # La plantilla viene con hoja "Items conteo"
    df = pd.read_excel(RUTA_COSTO_UNITARIO, sheet_name="Items conteo")
    df = limpiar_columnas(df)

    # Normalizamos nombres esperados
    # Esperamos al menos: seccion, item, costo_unitario (si tu excel usa otro nombre, lo mapeamos aquí)
    rename_map = {}
    for c in df.columns:
        if c in ["costo_unitario", "costo", "costo_unit", "costo_unitario_$"]:
            rename_map[c] = "costo_unitario"
        if c in ["seccion", "sección"]:
            rename_map[c] = "seccion"
    df = df.rename(columns=rename_map)

    # Validación mínima
    needed = {"seccion", "item", "costo_unitario"}
    if not needed.issubset(set(df.columns)):
        # Devuelve vacío para no romper el pipeline
        return pd.DataFrame()

    df["costo_unitario"] = pd.to_numeric(df["costo_unitario"], errors="coerce")
    df = df.dropna(subset=["seccion", "item", "costo_unitario"])

    # Merge para traer grupo_1 / grupo_2 desde dim_secciones
    if df_secciones_ref is not None and (not df_secciones_ref.empty) and "seccion" in df_secciones_ref.columns:
        cols = [c for c in ["seccion", "grupo_1", "grupo_2"] if c in df_secciones_ref.columns]
        df = df.merge(
            df_secciones_ref[cols].drop_duplicates("seccion"),
            on="seccion",
            how="left"
        )

    return df.reset_index(drop=True)
# =====================================================
# CALENDARIO
# =====================================================

def crear_calendario(df_ventas, df_gastos):

    fecha_min = min(df_ventas["fecha"].min(), df_gastos["fecha"].min())
    fecha_max = max(df_ventas["fecha"].max(), df_gastos["fecha"].max())

    calendario = pd.DataFrame({
        "fecha": pd.date_range(fecha_min, fecha_max)
    })

    calendario["anio"] = calendario["fecha"].dt.year
    calendario["mes"] = calendario["fecha"].dt.month
    calendario["mes_nombre"] = calendario["fecha"].dt.month_name()
    calendario["semana"] = calendario["fecha"].dt.isocalendar().week
    calendario["trimestre"] = calendario["fecha"].dt.quarter
    calendario["dia"] = calendario["fecha"].dt.day

    return calendario

# =====================================================
# COSTO UNITARIO (archivo cliente)
# =====================================================
def procesar_costo_unitario(df_secciones_ref=None):
    """
    Lee uploads/costo_unitario.xlsx
    Espera columnas tipo: seccion | item | costo_unitario
    """
    if not os.path.exists(RUTA_COSTO_UNITARIO):
        # tabla vacía (no rompe el ETL)
        return pd.DataFrame(columns=["seccion", "item", "costo_unitario"])

    df = pd.read_excel(RUTA_COSTO_UNITARIO)
    df = limpiar_columnas(df)

    # Normalizar nombres esperados (por si vienen con variaciones)
    # ejemplo: "costo_unitario", "costo unitario", etc.
    if "costo_unitario" not in df.columns:
        # intenta detectar columna de costo por heurística
        for c in df.columns:
            if "costo" in c and "unit" in c:
                df = df.rename(columns={c: "costo_unitario"})
                break

    # Validaciones mínimas
    needed = {"seccion", "item", "costo_unitario"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en costo_unitario.xlsx: {missing}")

    df["seccion"] = df["seccion"].astype(str).str.strip()
    df["item"] = df["item"].astype(str).str.strip()
    df["costo_unitario"] = pd.to_numeric(df["costo_unitario"], errors="coerce")

    df = df.dropna(subset=["seccion", "item", "costo_unitario"])
    df = df[df["costo_unitario"] >= 0]

    # Deduplicar: si el cliente repite item, nos quedamos con el último
    df = df.drop_duplicates(subset=["seccion", "item"], keep="last").reset_index(drop=True)

    return df


# =====================================================
# MAIN (soporta updates parciales)
# =====================================================

import argparse

def main(run_gastos: bool = True, run_ventas: bool = True, run_costos: bool = True):

    print("Conectando a PostgreSQL (Neon)...")
    DATABASE_URL = os.environ["DATABASE_URL"]
    engine = create_engine(DATABASE_URL)

    df_gastos = None
    df_ventas = None
    df_items = None
    df_secciones = None

    # -------------------------
    # GASTOS
    # -------------------------
    if run_gastos:
        print("Procesando Gastos...")
        df_gastos = procesar_gastos()
        df_gastos.to_sql("fact_gastos", engine, if_exists="replace", index=False)

    # -------------------------
    # VENTAS / ITEMS / SECCIONES
    # -------------------------
    if run_ventas:
        print("Procesando Transacciones...")
        df_ventas = procesar_transacciones()

        print("Procesando Items...")
        df_items = procesar_items()

        print("Procesando Secciones...")
        df_secciones = procesar_secciones()

        df_ventas.to_sql("fact_ventas", engine, if_exists="replace", index=False)
        df_items.to_sql("fact_items", engine, if_exists="replace", index=False)
        df_secciones.to_sql("dim_secciones", engine, if_exists="replace", index=False)

    # -------------------------
    # CALENDARIO (si cambió ventas o gastos)
    # -------------------------
    if run_ventas or run_gastos:
        print("Creando Calendario...")

        if df_ventas is None:
            df_ventas = pd.read_sql("SELECT * FROM fact_ventas", engine)
            if "fecha" in df_ventas.columns:
                df_ventas["fecha"] = pd.to_datetime(df_ventas["fecha"], errors="coerce")

        if df_gastos is None:
            df_gastos = pd.read_sql("SELECT * FROM fact_gastos", engine)
            if "fecha" in df_gastos.columns:
                df_gastos["fecha"] = pd.to_datetime(df_gastos["fecha"], errors="coerce")

        df_calendario = crear_calendario(df_ventas, df_gastos)
        df_calendario.to_sql("dim_calendario", engine, if_exists="replace", index=False)

    # -------------------------
    # COSTO UNITARIO (cliente)
    # -------------------------
    if run_costos:
        print("Procesando Costo Unitario (cliente)...")

        # necesitamos dim_secciones para traer grupo_1/grupo_2 si aplica
        if df_secciones is None:
            try:
                df_secciones = pd.read_sql("SELECT * FROM dim_secciones", engine)
            except Exception:
                df_secciones = pd.DataFrame()

        df_costo_unitario = procesar_costo_unitario(df_secciones_ref=df_secciones)

        if not df_costo_unitario.empty:
            df_costo_unitario.to_sql("dim_costos_unitarios", engine, if_exists="replace", index=False)
        else:
            print("⚠️ No se generó dim_costos_unitarios (archivo vacío o columnas inválidas).")

    print("ETL COMPLETADO CORRECTAMENTE.")


def run_etl():
    # modo clásico: corre todo
    main(True, True, True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ventas", action="store_true")
    parser.add_argument("--gastos", action="store_true")
    parser.add_argument("--costos", action="store_true")
    args = parser.parse_args()

    # si no pasan flags, corre todo
    if not (args.ventas or args.gastos or args.costos):
        main(True, True, True)
    else:
        main(run_gastos=args.gastos, run_ventas=args.ventas, run_costos=args.costos)
    
