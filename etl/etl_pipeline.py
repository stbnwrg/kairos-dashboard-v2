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
# MAIN
# =====================================================

def main():

    print("Procesando Gastos...")
    df_gastos = procesar_gastos()

    print("Procesando Transacciones...")
    df_ventas = procesar_transacciones()

    print("Procesando Items...")
    df_items = procesar_items()

    print("Procesando Secciones...")
    df_secciones = procesar_secciones()

    print("Creando Calendario...")
    df_calendario = crear_calendario(df_ventas, df_gastos)

    print("Guardando en PostgreSQL (Neon)...")

    DATABASE_URL = os.environ["DATABASE_URL"]

    engine = create_engine(DATABASE_URL)

    df_gastos.to_sql("fact_gastos", engine, if_exists="replace", index=False)
    df_ventas.to_sql("fact_ventas", engine, if_exists="replace", index=False)
    df_items.to_sql("fact_items", engine, if_exists="replace", index=False)
    df_secciones.to_sql("dim_secciones", engine, if_exists="replace", index=False)
    df_calendario.to_sql("dim_calendario", engine, if_exists="replace", index=False)

    print("ETL COMPLETADO CORRECTAMENTE.")


def run_etl():
    main()

if __name__ == "__main__":
    main()
    
