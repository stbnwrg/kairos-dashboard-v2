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
DATA_DIR = os.path.join(PROJECT_ROOT, "data")



# ---------------------------------
# GASTOS
# ---------------------------------
GASTOS_CANDIDATOS = [
    os.path.join(UPLOADS_DIR, "gastos.xlsx"),
    os.path.join(UPLOADS_DIR, "gastos.xls"),
    os.path.join(DATA_DIR, "gastos.xlsx"),
    os.path.join(DATA_DIR, "gastos.xls"),
]

RUTA_GASTOS = next(
    (p for p in GASTOS_CANDIDATOS if os.path.exists(p)),
    None
)

if RUTA_GASTOS:
    print("DEBUG RUTA_GASTOS:", RUTA_GASTOS)
else:
    print("⚠️ No se encontró archivo de gastos al iniciar ETL.")


# ---------------------------------
# VENTAS
# ---------------------------------
VENTAS_CANDIDATOS = [
    os.path.join(UPLOADS_DIR, "ventas.xlsx"),
    os.path.join(UPLOADS_DIR, "transacciones.xlsx"),
    os.path.join(DATA_DIR, "ventas.xlsx"),
    os.path.join(DATA_DIR, "transacciones.xlsx"),
]

RUTA_VENTAS = next(
    (p for p in VENTAS_CANDIDATOS if os.path.exists(p)),
    None
)

if RUTA_VENTAS:
    print("DEBUG RUTA_VENTAS:", RUTA_VENTAS)
else:
    print("⚠️ No se encontró archivo de ventas al iniciar ETL.")


# ---------------------------------
# COSTO UNITARIO
# ---------------------------------
COSTO_CANDIDATOS = [
    os.path.join(UPLOADS_DIR, "costo_unitario.xlsx"),
    os.path.join(UPLOADS_DIR, "costo unitario.xlsx"),
    os.path.join(DATA_DIR, "costo_unitario.xlsx"),
    os.path.join(DATA_DIR, "costo unitario.xlsx"),
]

RUTA_COSTO = next(
    (p for p in COSTO_CANDIDATOS if os.path.exists(p)),
    None
)

if RUTA_COSTO:
    print("DEBUG RUTA_COSTO:", RUTA_COSTO)
else:
    print("⚠️ No se encontró archivo de costo unitario al iniciar ETL.")

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
# ELIMINAR EMOJIS
# =====================================================
import re
import unicodedata

def normalizar_texto(texto: str) -> str:
    if pd.isna(texto):
        return ""

    # eliminar emojis y símbolos
    texto = re.sub(r"[^\w\s]", " ", str(texto))

    # eliminar tildes
    texto = unicodedata.normalize("NFKD", texto)
    texto = texto.encode("ascii", "ignore").decode("utf-8")

    return texto.upper().strip()

# =====================================================
# GASTOS
# =====================================================
# =====================================================
# GASTOS
# =====================================================
def procesar_gastos():

    if RUTA_GASTOS is None:
        raise FileNotFoundError("No se encontró archivo de gastos.")

    FECHA_INICIO_OPERACION = pd.Timestamp("2025-10-01")

    try:
        df = pd.read_excel(
            RUTA_GASTOS,
            sheet_name="Gastos",
            skiprows=1
        )

    except Exception:
        # fallback si cambia el nombre de la hoja
        df = pd.read_excel(
            RUTA_GASTOS,
            sheet_name=0,
            skiprows=1
        )

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
    if RUTA_VENTAS is None:
        raise FileNotFoundError("No se encontró archivo de ventas.")
    print("=== DEBUG PRODUCCION ===")
    print("CWD:", os.getcwd())
    print("Existe uploads/ventas.xlsx:", os.path.exists("uploads/ventas.xlsx"))
    print("Existe uploads/transacciones.xlsx:", os.path.exists("uploads/transacciones.xlsx"))
    print("Ruta ventas detectada:", RUTA_VENTAS)
    print("=========================")

    
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
    if RUTA_VENTAS is None:
        raise FileNotFoundError("No se encontró archivo de ventas.")
    
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

    if RUTA_VENTAS is None:
        raise FileNotFoundError("No se encontró archivo de ventas.")

    df = pd.read_excel(RUTA_VENTAS, sheet_name="Secciones", skiprows=1)
    df = limpiar_columnas(df)

    df["total"] = pd.to_numeric(df["total"], errors="coerce")
    df = df.dropna(subset=["seccion", "total"])

    # -----------------------
    # GRUPO 2
    # -----------------------
    def grupo_2(seccion):

        s = normalizar_texto(seccion)

        # -------------------------
        # PIZZA
        # -------------------------
        if "PIZZA" in s:
            return "PIZZA"

        # -------------------------
        # HELADOS
        # -------------------------
        if "HELADO" in s:
            return "HELADOS"

        # -------------------------
        # TÉ / TEA / TES
        # -------------------------
        if any(x in s for x in [" TE ", " TES", "TEA", "MATCHA", "INFUSION"]):
            return "TÉ"

        # -------------------------
        # BEBIDAS FRIAS
        # -------------------------
        if any(x in s for x in ["BEBIDA", "BATIDO", "JUGO", "FRIA", "FRIO", "ESPRESSO NARANJA"]):
            return "BEBIDAS FRIAS"

        # -------------------------
        # CAFÉ
        # -------------------------
        if "CAFE" in s:
            return "CAFÉ"

        # -------------------------
        # PASTELERÍA
        # -------------------------
        if any(x in s for x in ["PASTEL", "CROISSANT", "WAFFLE", "BOLLERIA", "GALLET", "CHEESECAKE", "TORTA"]):
            return "PASTELERÍA"

        # -------------------------
        # SANDWICH
        # -------------------------
        if "SANDWICH" in s:
            return "SANDWICH"

        # -------------------------
        # PROMOCIONES
        # -------------------------
        if "PROMOCION" in s:
            return "PROMOCIONES"

        return "OTROS"

    df["grupo_2"] = df["seccion"].apply(grupo_2)

    # -----------------------
    # GRUPO 1
    # -----------------------
    def grupo_1(row):
        if row["grupo_2"] in ["CAFÉ", "PASTELERÍA", "TÉ", "PIZZA", "SANDWICH"]:
            return row["grupo_2"]
        return "OTROS"

    df["grupo_1"] = df.apply(grupo_1, axis=1)

    return df.reset_index(drop=True)

# =====================================================
# PROCESAR COSTOS UNITARIOS
# =====================================================

def procesar_costo_unitario():

    if RUTA_COSTO is None:
        print("No existe archivo costo unitario.xlsx")
        return pd.DataFrame(columns=["seccion", "item", "costo_unitario"])

    # NO forzamos nombre de hoja
    try:
        df = pd.read_excel(RUTA_COSTO, sheet_name="Items conteo")
    except Exception:
    # fallback: primera hoja si el nombre cambia
        df = pd.read_excel(RUTA_COSTO)

    print("Primeras filas crudas:")
    print(df.head())

    df = limpiar_columnas(df)

    print("Columnas detectadas:", df.columns.tolist())

    # Asegurar encabezados correctos
    if "costo_unitario" not in df.columns:
        for c in df.columns:
            if "costo" in c:
                df = df.rename(columns={c: "costo_unitario"})
                break

    columnas_necesarias = {"seccion", "item", "costo_unitario"}
    faltantes = columnas_necesarias - set(df.columns)

    if faltantes:
        print("Faltan columnas:", faltantes)
        return pd.DataFrame(columns=["seccion", "item", "costo_unitario"])

    # Limpieza datos
    df["seccion"] = df["seccion"].astype(str).str.strip()
    df["item"] = df["item"].astype(str).str.strip()

    df["costo_unitario"] = pd.to_numeric(
        df["costo_unitario"],
        errors="coerce"
    ).fillna(0)

    df = df.dropna(subset=["seccion", "item"])
    df = df.drop_duplicates(subset=["seccion", "item"], keep="last")

    print("Filas finales costo_unitario:", len(df))

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

        df_costo_unitario = procesar_costo_unitario()

        if not df_costo_unitario.empty:
            df_costo_unitario.to_sql(
                "dim_costos_unitarios",
                engine,
                if_exists="replace",
                index=False
            )
        else:
            print("⚠️ No se generó dim_costos_unitarios.")

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
    
