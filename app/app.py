import streamlit as st
import pandas as pd
import sqlite3
import os
import plotly.express as px
import subprocess
import tempfile
import datetime
import base64
import io
# import pdfkit

#WKHTML_PATH = r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
#config = pdfkit.configuration(wkhtmltopdf=WKHTML_PATH)


# ======================================================
# CONFIG
# ======================================================
st.set_page_config(page_title="Café Kairos - Executive Dashboard", layout="wide")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "database", "kairos.db")

# Crear carpeta database si no existe (necesario en Render)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

MESES_CORTO = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr",
    5: "may", 6: "jun", 7: "jul", 8: "ago",
    9: "sep", 10: "oct", 11: "nov", 12: "dic"
}

def period_label_from_period(p) -> str:
    """
    p: pandas Period('2025-10', 'M')
    retorna: 'oct-25'
    """
    try:
        return f"{MESES_CORTO[int(p.month)]}-{str(p.year)[-2:]}"
    except Exception:
        return str(p)

def enforce_date_only(df: pd.DataFrame, col: str = "fecha") -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df

KAIROS_CAFE = "#4B2E2B"
KAIROS_BEIGE = "#EFE7DE"
KAIROS_BG = "#F6F3EF"
KAIROS_GOLD = "#C8A97E"
KAIROS_MUTED = "#D7C2B0"

TAX_RATE = 0.27  # impuesto corporativo referencial

# ======================================================
# LOGIN SIMPLE
# ======================================================

import os

APP_PASSWORD = os.environ.get("APP_PASSWORD", "kairos2025")

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("🔐 Acceso Dashboard Kairos")

    password_input = st.text_input("Ingrese contraseña", type="password")

    if st.button("Ingresar"):
        if password_input == APP_PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Contraseña incorrecta")

    st.stop()


# ======================================================
# ESTILO (UI)
# ======================================================
st.markdown(f"""
<style>

/* ===== TABLA ESTADO RESULTADOS HISTÓRICO ===== */

div[data-testid="stDataFrame"] div[role="columnheader"] {{
    background-color: #4B2E2B !important;
    color: white !important;
    font-weight: 700 !important;
    text-align: center !important;
}}

div[data-testid="stDataFrame"] div[role="gridcell"]:first-child {{
    background-color: #EFE7DE !important;
    color: #4B2E2B !important;
    font-weight: 700 !important;
}}
            
            
.main {{ background-color: {KAIROS_BG}; }}

section[data-testid="stSidebar"] {{
    background-color: {KAIROS_BEIGE};
}}

/* BOTONES SIDEBAR: café seleccionado, beige no seleccionado */
section[data-testid="stSidebar"] button {{
    border-radius: 10px !important;
    font-weight: 650 !important;
}}

section[data-testid="stSidebar"] button[kind="primary"] {{
    background-color: {KAIROS_CAFE} !important;
    color: #FFFFFF !important;
    border: 1px solid rgba(0,0,0,0.0) !important;
}}

section[data-testid="stSidebar"] button[kind="secondary"] {{
    background-color: {KAIROS_BEIGE} !important;
    color: {KAIROS_CAFE} !important;
    border: 1px solid {KAIROS_MUTED} !important;
}}


/* KPI cards */
div[data-testid="stMetric"] {{
  background: #4B2E2B;
  padding: 20px;
  border-radius: 18px;
  border: 1px solid rgba(255,255,255,0.08);
  text-align: center !important;
}}

div[data-testid="stMetric"] label {{
  color: rgba(255,255,255,0.85) !important;
  font-weight: 600 !important;
  text-align: center !important;
  width: 100%;
  display: block;
}}

div[data-testid="stMetric"] div {{
  color: #FFFFFF !important;
  font-weight: 900 !important;
  font-size: 28px !important;
  text-align: center !important;
}}


/* Autoajuste tablas */
div[data-testid="stDataFrame"] {{
    width: 100% !important;
}}

div[data-testid="stDataFrame"] table {{
    width: 100% !important;
}}

div[data-testid="stDataFrame"] th {{
    text-align: center !important;
}}

</style>
""", unsafe_allow_html=True)

# ======================================================
# DATA
# ======================================================


# Ejecutar ETL si la DB no existe
def ensure_database():
    if not os.path.exists(DB_PATH):
        try:
            from etl.etl_pipeline import run_etl
            run_etl()
        except Exception as e:
            st.warning("No se pudo generar base de datos. Esperando carga de archivos.")
ensure_database()

@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_PATH)
    ventas = pd.read_sql("SELECT * FROM fact_ventas", conn)
    gastos = pd.read_sql("SELECT * FROM fact_gastos", conn)
    items = pd.read_sql("SELECT * FROM fact_items", conn)
    secciones = pd.read_sql("SELECT * FROM dim_secciones", conn)
    try:
        calendario = pd.read_sql("SELECT * FROM dim_calendario", conn)
    except Exception:
        calendario = pd.DataFrame()
    conn.close()
    return ventas, gastos, items, secciones, calendario

ventas, gastos, items, secciones, calendario = load_data()

# fechas
for df, col in [(ventas, "fecha"), (gastos, "fecha"), (items, "fecha")]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")


# ======================================================
# HELPERS
# ======================================================
def fmt_money(x) -> str:
    try:
        return f"${float(x):,.0f}".replace(",", ".")
    except Exception:
        return "$0"

# styler Kairos/ formateador miles
def _fmt_miles(x) -> str:
    v = pd.to_numeric(x, errors="coerce")
    if pd.isna(v):
        return ""
    return f"{int(v):,}".replace(",", ".")

def ensure_session_list(key: str, default_list: list[int]):
    if key not in st.session_state:
        st.session_state[key] = list(default_list)


def toggle_in_list(key: str, value: int):
    cur = list(st.session_state.get(key, []))
    if value in cur:
        cur.remove(value)
    else:
        cur.append(value)
    st.session_state[key] = cur


def set_list(key: str, values: list[int]):
    st.session_state[key] = list(values)


def years_available(df_dates: pd.DataFrame) -> list[int]:
    if df_dates.empty:
        return []
    return sorted(df_dates["year"].unique().tolist())


def months_available_for_years(df_dates: pd.DataFrame, years_selected: list[int]) -> list[int]:
    if df_dates.empty or not years_selected:
        return []
    tmp = df_dates[df_dates["year"].isin(years_selected)]
    return sorted(tmp["month"].unique().tolist())


def first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def fig_to_base64_png(fig, width=1200, height=600) -> str:
    """
    Convierte un Plotly Figure a PNG en base64.
    Requiere kaleido instalado: pip install -U kaleido
    """
    png_bytes = fig.to_image(format="png", width=width, height=height)
    return base64.b64encode(png_bytes).decode("utf-8")

def generar_pdf_html(
    ventas_total,
    costos_variables,
    costos_fijos,
    ebit,
    impuestos,
    resultado_neto,
    margen_operacional,
    punto_equilibrio,
    fig_resultado,
    fig_flujo,
    fig_acum,
    hist_pl,
    estado_df,
    filtros_label=""
):

    # Convertir gráficos a base64
    img_res = fig_to_base64_png(fig_resultado)
    img_flujo = fig_to_base64_png(fig_flujo)
    img_acum = fig_to_base64_png(fig_acum)

    # Tablas HTML
    estado_html = estado_df.to_html(index=False)
    hist_html = hist_pl.to_html()

    html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                font-family: Arial, sans-serif;
                background-color: {KAIROS_BG};
                margin: 40px;
                color: #222;
            }}

            h1 {{
                text-align:center;
                color: {KAIROS_CAFE};
                margin-bottom: 5px;
            }}

            h2 {{
                margin-top: 40px;
                color: {KAIROS_CAFE};
            }}

            .kpi-container {{
                display: flex;
                justify-content: space-between;
                margin-top: 25px;
                margin-bottom: 35px;
            }}

            .kpi {{
                background: {KAIROS_CAFE};
                color: white;
                padding: 15px;
                border-radius: 12px;
                width: 18%;
                text-align: center;
                font-weight: bold;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
                font-size: 12px;
            }}

            th, td {{
                border: 1px solid #ddd;
                padding: 6px;
                text-align: right;
            }}

            th {{
                background-color: {KAIROS_BEIGE};
                color: {KAIROS_CAFE};
                text-align: center;
            }}

            .center {{
                text-align:center;
            }}

            img {{
                margin-top: 15px;
                margin-bottom: 25px;
            }}

            .page-break {{
                page-break-after: always;
            }}
        </style>
    </head>
    <body>

        <h1>Café Kairos</h1>
        <div class="center">Executive Dashboard (Versión PDF)</div>
        <div class="center">{filtros_label}</div>

        <div class="kpi-container">
            <div class="kpi">Ventas<br>{fmt_money(ventas_total)}</div>
            <div class="kpi">Costos Var<br>{fmt_money(costos_variables)}</div>
            <div class="kpi">Costos Fijos<br>{fmt_money(costos_fijos)}</div>
            <div class="kpi">Margen<br>{margen_operacional:.1%}</div>
            <div class="kpi">Punto Equilibrio<br>{fmt_money(punto_equilibrio)}</div>
        </div>

        <h2>Resultado Mensual Histórico</h2>
        <img src="data:image/png;base64,{img_res}" width="100%">

        <h2>Flujo Neto</h2>
        <img src="data:image/png;base64,{img_flujo}" width="100%">

        <h2>Flujo Acumulado</h2>
        <img src="data:image/png;base64,{img_acum}" width="100%">

        <div class="page-break"></div>

        <h2>Estado de Resultados (Filtro actual)</h2>
        {estado_html}

        <h2>Estado de Resultados Histórico</h2>
        {hist_html}

    </body>
    </html>
    """

    pdf_path = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf").name

    # pdfkit.from_string(
    #    html,
    #    pdf_path,
    #    configuration=config,
    #    options={
    #        "enable-local-file-access": None
    #    }
    #)

    return pdf_path

# ======================================================
# HEADER
# ======================================================
col1, col2, col3 = st.columns([1, 4, 1])

with col1:
    st.image(os.path.join(BASE_DIR, "assets", "logo_kairos.jpg"), width=220)

with col2:
    st.markdown("<h1 style='text-align:center;margin-bottom:0;'>Café Kairos</h1>", unsafe_allow_html=True)
    st.markdown(f"<h4 style='text-align:center;color:{KAIROS_CAFE};margin-top:4px;'>Executive Dashboard</h4>",
                unsafe_allow_html=True)

with col3:
    st.image(os.path.join(BASE_DIR, "assets", "logo_estero.png"), width=160)

st.divider()

# ======================================================
# CARGA DE ARCHIVOS + RECARGA ETL
# ======================================================

st.sidebar.markdown("## Actualizar Datos")

ventas_file = st.sidebar.file_uploader(
    "Subir archivo Ventas",
    type=["xlsx"],
    key="ventas_upload"
)

gastos_file = st.sidebar.file_uploader(
    "Subir archivo Gastos",
    type=["xlsx"],
    key="gastos_upload"
)

if st.sidebar.button("🔄 Procesar y Recargar", use_container_width=True):

    if ventas_file is None or gastos_file is None:
        st.sidebar.error("Debe subir ambos archivos.")
    else:
        with st.spinner("Procesando archivos..."):

            os.makedirs("uploads", exist_ok=True)

            ventas_path = os.path.join("uploads", "ventas.xlsx")
            gastos_path = os.path.join("uploads", "gastos.xlsx")

            with open(ventas_path, "wb") as f:
                f.write(ventas_file.getbuffer())

            with open(gastos_path, "wb") as f:
                f.write(gastos_file.getbuffer())

            # Ejecutar ETL
            subprocess.run(["python", "etl/etl_pipeline.py"])

            st.cache_data.clear()
            st.success("Datos actualizados correctamente.")
            st.rerun()

# ======================================================
# CALENDARIO REAL (sin inventar meses)
# (union fechas ventas + gastos)
# ======================================================
date_series = pd.concat(
    [
        ventas["fecha"].dropna() if "fecha" in ventas.columns else pd.Series(dtype="datetime64[ns]"),
        gastos["fecha"].dropna() if "fecha" in gastos.columns else pd.Series(dtype="datetime64[ns]")
    ],
    ignore_index=True
)

df_dates = pd.DataFrame({"fecha": date_series})
if not df_dates.empty:
    df_dates["year"] = df_dates["fecha"].dt.year
    df_dates["month"] = df_dates["fecha"].dt.month

all_years = years_available(df_dates)

# ======================================================
# FILTROS (BOTONES)
# ======================================================

# --- AÑOS
st.sidebar.markdown("### Año")

ensure_session_list("years_sel", all_years)
# limpiar inválidos si cambió el calendario
st.session_state["years_sel"] = [y for y in st.session_state["years_sel"] if y in all_years]
if not st.session_state["years_sel"] and all_years:
    st.session_state["years_sel"] = list(all_years)

c1, c2 = st.sidebar.columns(2)
if c1.button("Seleccionar todo", type="secondary", use_container_width=True, key="years_all_btn"):
    set_list("years_sel", all_years)
if c2.button("Limpiar", type="secondary", use_container_width=True, key="years_clear_btn"):
    set_list("years_sel", [])
    
# Centrar Filtro de año en panel
years_cols = st.sidebar.columns([1,1,1])

for i, y in enumerate(all_years):
    selected = (y in st.session_state["years_sel"])
    btn_type = "primary" if selected else "secondary"

    years_cols[i % 3].markdown(
        "<div style='text-align:center;'>",
        unsafe_allow_html=True
    )

    if years_cols[i % 3].button(
        str(y),
        type=btn_type,
        use_container_width=True,
        key=f"year_{y}"
    ):
        toggle_in_list("years_sel", y)

    years_cols[i % 3].markdown("</div>", unsafe_allow_html=True)

years_sel = sorted(st.session_state["years_sel"])

# --- MESES (disponibles según años)
available_months = months_available_for_years(df_dates, years_sel)

st.sidebar.markdown("### Mes")

ensure_session_list("months_sel", available_months if available_months else [])
# mantener solo meses válidos
st.session_state["months_sel"] = [m for m in st.session_state["months_sel"] if m in available_months]
if not st.session_state["months_sel"] and available_months:
    st.session_state["months_sel"] = list(available_months)

m1, m2 = st.sidebar.columns(2)
if m1.button("Seleccionar todo", type="secondary", use_container_width=True, key="months_all_btn"):
    set_list("months_sel", available_months)
if m2.button("Limpiar", type="secondary", use_container_width=True, key="months_clear_btn"):
    set_list("months_sel", [])

mes_cols = st.sidebar.columns(2)  # 2 columnas para que no se rompa Septiembre
for idx, m in enumerate(available_months):
    selected = (m in st.session_state["months_sel"])
    btn_type = "primary" if selected else "secondary"
    label = MESES.get(m, str(m))
    if mes_cols[idx % 2].button(label, type=btn_type, use_container_width=True, key=f"month_{m}"):
        toggle_in_list("months_sel", m)

months_sel = sorted(st.session_state["months_sel"])

# (IMPORTANTE) eliminamos el texto de años/meses bajo los filtros -> NO LO MOSTRAMOS

# ======================================================
# APLICAR FILTROS (dinámicos)
# Aplica a: KPI, donut, explorador, estado de resultados
# No aplica a: Resultado mensual histórico, Flujo histórico
# ======================================================
ventas_f = ventas.copy()
gastos_f = gastos.copy()
items_f = items.copy()

if years_sel and months_sel:
    if "fecha" in ventas_f.columns:
        ventas_f = ventas_f[
            (ventas_f["fecha"].dt.year.isin(years_sel)) &
            (ventas_f["fecha"].dt.month.isin(months_sel))
        ]
    if "fecha" in gastos_f.columns:
        gastos_f = gastos_f[
            (gastos_f["fecha"].dt.year.isin(years_sel)) &
            (gastos_f["fecha"].dt.month.isin(months_sel))
        ]
    if "fecha" in items_f.columns:
        items_f = items_f[
            (items_f["fecha"].dt.year.isin(years_sel)) &
            (items_f["fecha"].dt.month.isin(months_sel))
        ]
else:
    # si limpiaste todo -> vacío (honesto)
    ventas_f = ventas_f.iloc[0:0]
    gastos_f = gastos_f.iloc[0:0]
    items_f = items_f.iloc[0:0]

# ======================================================
# KPIs (Operativo) + KPI Inversión separado
# Implementación/Inversión NO entra al operativo
# ======================================================
ventas_total = float(ventas_f["total"].sum()) if "total" in ventas_f.columns else 0.0

# Si el ETL está OK, tenemos grupo_3: COSTO VARIABLE / COSTO FIJO / INVERSION
if "grupo_3" in gastos_f.columns and "total" in gastos_f.columns:
    costos_variables = float(gastos_f[gastos_f["grupo_3"] == "COSTO VARIABLE"]["total"].sum())
    costos_fijos = float(gastos_f[gastos_f["grupo_3"] == "COSTO FIJO"]["total"].sum())
    inversion = float(gastos_f[gastos_f["grupo_3"] == "INVERSION"]["total"].sum())
else:
    # fallback ultra defensivo
    costos_variables = float(gastos_f["total"].sum()) if "total" in gastos_f.columns else 0.0
    costos_fijos = 0.0
    inversion = 0.0

ebit = ventas_total - costos_variables - costos_fijos

# EBITDA: sin depreciación/amortización cargada, asumimos 0 (quedará igual a EBIT)
ebitda = ebit
impuestos = (ebit * TAX_RATE) if ebit > 0 else 0.0
resultado_neto = ebit - impuestos
margen_operacional = (ebit / ventas_total) if ventas_total else 0.0

# ======================================================
# KPIs (Operativo) + KPI Inversión separado
# ======================================================

# ... (tu cálculo de ventas_total, costos_variables, costos_fijos, inversion, ebit, etc. queda igual)

st.markdown("""
<style>
.kpi-row{
  display:flex;
  gap:14px;
  justify-content:center;
  align-items:stretch;
  flex-wrap:wrap;
  margin-top: 6px;
}
.kpi-card{
  background: #4B2E2B;
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 16px;
  padding: 14px 14px;
  width: 210px;              /* ancho fijo (tarjeta) */
  min-height: 86px;
  text-align:center;
  box-sizing:border-box;
}
.kpi-title{
  color: rgba(255,255,255,0.90);
  font-weight: 650;
  line-height: 1.05;
  font-size: clamp(12px, 1.1vw, 16px);   /* autoajuste */
  margin-bottom: 6px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.kpi-value{
  color:#FFFFFF;
  font-weight: 900;
  line-height: 1.0;
  font-size: clamp(16px, 1.6vw, 26px);   /* autoajuste */
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
</style>
""", unsafe_allow_html=True)

kpis = [
    ("Ventas", fmt_money(ventas_total)),
    ("Costos Variables", fmt_money(costos_variables)),
    ("Costos Fijos", fmt_money(costos_fijos)),
    ("Margen Operacional", f"{margen_operacional:.1%}"),
    ("Inversión (Implementación)", fmt_money(inversion)),
]

cards_html = '<div class="kpi-row">' + "".join(
    [f'<div class="kpi-card"><div class="kpi-title">{t}</div><div class="kpi-value">{v}</div></div>' for t, v in kpis]
) + "</div>"

st.markdown(cards_html, unsafe_allow_html=True)

st.divider()

# ======================================================
# Semáforo Margen
# ======================================================

def semaforo_margen(m):
    if m > 0.20:
        return "🟢 Margen saludable"
    elif m > 0.10:
        return "🟡 Margen aceptable"
    else:
        return "🔴 Margen crítico"

estado_margen = semaforo_margen(margen_operacional)

color_margen = (
    "#2E7D32" if margen_operacional > 0.20 else
    "#F9A825" if margen_operacional > 0.10 else
    "#C62828"
)

st.markdown(
    f"""
    <div style='
        padding:15px;
        border-radius:12px;
        background-color:{color_margen};
        color:white;
        font-weight:600;
        text-align:center;
        font-size:18px;
    '>
        {estado_margen}
    </div>
    """,
    unsafe_allow_html=True
)
# ======================================================
# KPI YoY (Comparativo año anterior)
# ======================================================

st.divider()
st.subheader("Comparativo YoY (mismo período)")

# Definir período actual
if years_sel and months_sel:
    current_mask = (
        (ventas["fecha"].dt.year.isin(years_sel)) &
        (ventas["fecha"].dt.month.isin(months_sel))
    )
else:
    current_mask = ventas["fecha"].notna()

ventas_actual = ventas[current_mask]["total"].sum()

# Año anterior
years_prev = [y - 1 for y in years_sel]
prev_mask = (
    (ventas["fecha"].dt.year.isin(years_prev)) &
    (ventas["fecha"].dt.month.isin(months_sel))
)

ventas_prev = ventas[prev_mask]["total"].sum()

crecimiento = ((ventas_actual - ventas_prev) / ventas_prev) if ventas_prev else 0

col_y1, col_y2 = st.columns(2)

col_y1.metric(
    "Ventas vs Año Anterior",
    fmt_money(ventas_actual),
    f"{crecimiento:.1%}"
)

# EBIT YoY
ebit_actual = ebit
# calcular EBIT año anterior
gastos_prev = gastos[
    (gastos["fecha"].dt.year.isin(years_prev)) &
    (gastos["fecha"].dt.month.isin(months_sel)) &
    (gastos["grupo_3"] != "INVERSION")
]

ventas_prev_ebit = ventas[prev_mask]["total"].sum()
costos_prev_ebit = gastos_prev["total"].sum()
ebit_prev = ventas_prev_ebit - costos_prev_ebit

crec_ebit = ((ebit_actual - ebit_prev) / abs(ebit_prev)) if ebit_prev else 0

col_y2.metric(
    "EBIT vs Año Anterior",
    fmt_money(ebit_actual),
    f"{crec_ebit:.1%}"
)
# ======================================================
# RESULTADO MENSUAL (HISTÓRICO) - NO FILTRADO
# Excluye inversión del costo (operativo)
# ======================================================
col_graf1, col_graf2 = st.columns(2)

with col_graf1:
    st.subheader("Resultado Mensual (Histórico)")

    if "fecha" in ventas.columns and "total" in ventas.columns:
        ventas_mensual = ventas.groupby(
            ventas["fecha"].dt.to_period("M")
        )["total"].sum()
    else:
        ventas_mensual = pd.Series(dtype=float)

    if "fecha" in gastos.columns and "total" in gastos.columns:
        if "grupo_3" in gastos.columns:
            gastos_base = gastos[gastos["grupo_3"] != "INVERSION"]
        else:
            gastos_base = gastos
        gastos_mensual = gastos_base.groupby(
            gastos_base["fecha"].dt.to_period("M")
        )["total"].sum()
    else:
        gastos_mensual = pd.Series(dtype=float)

    resultado_hist = (
        pd.DataFrame(
            {"Ventas": ventas_mensual, "Costos": gastos_mensual}
        )
        .fillna(0)
        .sort_index()
    )

    # Labels español corto
    resultado_hist.index.name = "periodo"
    periodos_idx = resultado_hist.index
    labels = [period_label_from_period(p) for p in periodos_idx]

    resultado_hist_plot = resultado_hist.copy()
    resultado_hist_plot["Periodo"] = labels

    fig_res = px.bar(
        resultado_hist_plot,
        x="Periodo",
        y=["Ventas", "Costos"],
        barmode="group",
        color_discrete_map={
            "Ventas": KAIROS_GOLD,
            "Costos": KAIROS_CAFE
        },
    )

    fig_res.update_layout(
        plot_bgcolor=KAIROS_BG,
        paper_bgcolor=KAIROS_BG,
        legend_title_text="",
        xaxis=dict(type="category", title=""),
        yaxis=dict(title="")
    )

    st.plotly_chart(fig_res, use_container_width=True)

# ======================================================
# TOP 5 SECCIONES (donut) - FILTRADO
# ======================================================
with col_graf2:
    st.subheader("Top 5 Secciones (Filtro)")

    if items_f.empty or "seccion" not in items_f.columns:
        st.info("No hay datos para el filtro actual.")
    else:
        col_val = "precio" if "precio" in items_f.columns else ("total" if "total" in items_f.columns else None)
        if col_val is None:
            st.warning("No encuentro columna de valor (precio/total) en fact_items.")
        else:
            top5 = (
                items_f.groupby("seccion")[col_val]
                .sum()
                .sort_values(ascending=False)
                .head(5)
            )

            if top5.empty:
                st.info("No hay datos para el filtro actual.")
            else:
                fig_donut = px.pie(
                    names=top5.index.astype(str),
                    values=top5.values,
                    hole=0.6,
                    color_discrete_sequence=[KAIROS_CAFE, KAIROS_GOLD, KAIROS_MUTED, "#A67C52", "#6F4E37"]
                )
                fig_donut.update_layout(plot_bgcolor=KAIROS_BG, paper_bgcolor=KAIROS_BG, legend_title_text="")
                st.plotly_chart(fig_donut, use_container_width=True)

# ======================================================
# EXPLORADOR: VENTAS / COSTOS (por grupos) - FILTRADO
# ======================================================
st.divider()
st.subheader("Explorador de Ventas y Costos (por grupos)")

tab_v, tab_c = st.tabs(["🧾 Ventas", "🧰 Costos"])


# ======================================================
# ======================= VENTAS =======================
# ======================================================
with tab_v:
    if items_f.empty:
        st.info("No hay ventas para el filtro actual.")
    else:
        dfv = items_f.copy()

        if not secciones.empty and "seccion" in dfv.columns and "seccion" in secciones.columns:
            sec_cols = [c for c in ["seccion", "grupo_1", "grupo_2"] if c in secciones.columns]
            dfv = dfv.merge(
                secciones[sec_cols].drop_duplicates("seccion"),
                on="seccion",
                how="left",
                suffixes=("", "_sec")
            )

        dfv = dfv.loc[:, ~dfv.columns.duplicated()]

        val_col = "precio" if "precio" in dfv.columns else ("total" if "total" in dfv.columns else None)

        if val_col is None:
            st.warning("No encuentro columna de valor (precio/total) en fact_items.")
        else:
            group_choice = st.radio(
                "Agrupar ventas por",
                options=[c for c in ["grupo_1", "grupo_2", "seccion"] if c in dfv.columns],
                horizontal=True
            )

            ventas_grp = (
                dfv.groupby(group_choice)[val_col]
                .sum()
                .sort_values(ascending=False)
                .head(15)
                .reset_index()
            )

            col_chart, col_table = st.columns([1.2, 1])

            # --------- Gráfico ----------
            with col_chart:
                fig_vgrp = px.bar(
                    ventas_grp,
                    x=val_col,
                    y=group_choice,
                    orientation="h",
                    color_discrete_sequence=[KAIROS_CAFE],
                )
                fig_vgrp.update_layout(
                    plot_bgcolor=KAIROS_BG,
                    paper_bgcolor=KAIROS_BG,
                    yaxis_title="",
                    xaxis_title="",
                    height=520
                )
                st.plotly_chart(fig_vgrp, use_container_width=True)

            # --------- Tabla ----------
            with col_table:
                st.markdown("**Detalle (Top 50)**")

                cols_to_show = list(dict.fromkeys([group_choice, val_col, "fecha", "seccion"]))

                detalle = (
                    dfv[[c for c in cols_to_show if c in dfv.columns]]
                    .sort_values(val_col, ascending=False)
                    .head(50)
                    .reset_index(drop=True)
                )

                detalle = enforce_date_only(detalle, "fecha")

                sty = (
                    detalle.style
                    .hide(axis="index")
                    .format({val_col: _fmt_miles})
                    .set_table_styles([
                        {
                            "selector": "th",
                            "props": [
                                ("background-color", KAIROS_CAFE),
                                ("color", "white"),
                                ("text-align", "center"),
                                ("font-weight", "700"),
                            ],
                        },
                        {
                            "selector": "td",
                            "props": [("text-align", "center")],
                        },
                    ])
                )

                st.markdown(
                    f"""
                    <div style="
                        height:520px;
                        overflow-y:auto;
                        border:1px solid #DDD;
                        border-radius:10px;
                        padding:6px;
                        background:white;
                    ">
                        {sty.to_html()}
                    </div>
                    """,
                    unsafe_allow_html=True
                )


# ======================================================
# ======================= COSTOS =======================
# ======================================================
with tab_c:
    if gastos_f.empty:
        st.info("No hay costos para el filtro actual.")
    else:
        dfc = gastos_f.copy()

        if "total" not in dfc.columns:
            st.warning("No encuentro columna 'total' en fact_gastos.")
        else:
            show_inversion_detail = st.toggle(
                "Mostrar detalle de inversión (implementación)",
                value=False
            )

            available_groups = [c for c in ["grupo_1", "grupo_2", "grupo_3", "tipo"] if c in dfc.columns]

            group_col = st.radio(
                "Agrupar costos por",
                options=available_groups,
                horizontal=True
            )

            tipo = st.radio(
                "Tipo de costo (operativo)",
                ["Todo (sin inversión)", "Solo Variables", "Solo Fijos"],
                horizontal=True
            )

            df_oper = dfc.copy()

            if "grupo_3" in df_oper.columns:
                df_oper = df_oper[df_oper["grupo_3"] != "INVERSION"]

                if tipo == "Solo Variables":
                    df_oper = df_oper[df_oper["grupo_3"] == "COSTO VARIABLE"]
                elif tipo == "Solo Fijos":
                    df_oper = df_oper[df_oper["grupo_3"] == "COSTO FIJO"]

            costos_grp = (
                df_oper.groupby(group_col)["total"]
                .sum()
                .sort_values(ascending=False)
                .head(15)
                .reset_index()
            )

            col_chart_c, col_table_c = st.columns([1.2, 1])

            # --------- Gráfico ----------
            with col_chart_c:
                fig_cgrp = px.bar(
                    costos_grp,
                    x="total",
                    y=group_col,
                    orientation="h",
                    color_discrete_sequence=[KAIROS_GOLD],
                )
                fig_cgrp.update_layout(
                    plot_bgcolor=KAIROS_BG,
                    paper_bgcolor=KAIROS_BG,
                    yaxis_title="",
                    xaxis_title="",
                    height=520
                )
                st.plotly_chart(fig_cgrp, use_container_width=True)

            # --------- Tabla ----------
            with col_table_c:
                st.markdown("**Detalle (Top 50 - operativo, sin inversión)**")

                cols_to_show = list(dict.fromkeys([group_col, "total", "fecha", "tipo", "grupo_3"]))

                detalle_costos = (
                    df_oper[[c for c in cols_to_show if c in df_oper.columns]]
                    .sort_values("total", ascending=False)
                    .head(50)
                    .reset_index(drop=True)
                )

                detalle_costos = enforce_date_only(detalle_costos, "fecha")

                sty_costos = (
                    detalle_costos.style
                    .hide(axis="index")
                    .format({"total": _fmt_miles})
                    .set_table_styles([
                        {
                            "selector": "th",
                            "props": [
                                ("background-color", KAIROS_CAFE),
                                ("color", "white"),
                                ("text-align", "center"),
                                ("font-weight", "700"),
                            ],
                        },
                        {
                            "selector": "td",
                            "props": [("text-align", "center")],
                        },
                    ])
                )

                st.markdown(
                    f"""
                    <div style="
                        height:520px;
                        overflow-y:auto;
                        border:1px solid #DDD;
                        border-radius:10px;
                        padding:6px;
                        background:white;
                    ">
                        {sty_costos.to_html()}
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            # ================= INVERSIÓN =================
            if show_inversion_detail and "grupo_3" in dfc.columns:
                st.markdown("**Detalle Inversión (Implementación)**")

                dfi = dfc[dfc["grupo_3"] == "INVERSION"].copy()

                if dfi.empty:
                    st.info("No hay inversión para el filtro actual.")
                else:
                    cols_i = list(dict.fromkeys([group_col, "total", "fecha", "tipo"]))

                    detalle_inv = (
                        dfi[[c for c in cols_i if c in dfi.columns]]
                        .sort_values("total", ascending=False)
                        .head(50)
                        .reset_index(drop=True)
                    )

                    detalle_inv = enforce_date_only(detalle_inv, "fecha")

                    sty_inv = (
                        detalle_inv.style
                        .hide(axis="index")
                        .format({"total": _fmt_miles})
                        .set_table_styles([
                            {
                                "selector": "th",
                                "props": [
                                    ("background-color", KAIROS_CAFE),
                                    ("color", "white"),
                                    ("text-align", "center"),
                                    ("font-weight", "700"),
                                ],
                            },
                            {
                                "selector": "td",
                                "props": [("text-align", "center")],
                            },
                        ])
                    )

                    st.markdown(sty_inv.to_html(), unsafe_allow_html=True)

# ======================================================
# FLUJO (HISTÓRICO) - NO FILTRADO
# Flujo Neto = Ventas - Costos operativos (sin inversión)
# ======================================================
st.divider()
st.subheader("Flujo de Caja Operativo (Histórico)")

# Reusamos resultado_hist (histórico mensual no filtrado)
if resultado_hist.empty:
    st.info("No hay histórico suficiente para mostrar flujo.")
else:
    flujo = resultado_hist.copy()
flujo["Flujo Neto"] = flujo["Ventas"] - flujo["Costos"]
flujo["Flujo Acumulado"] = flujo["Flujo Neto"].cumsum()

# labels mensuales español
periodos_idx = flujo.index
labels = [period_label_from_period(p) for p in periodos_idx]
flujo_plot = flujo.copy()
flujo_plot["Periodo"] = labels

fig_flujo = px.bar(
    flujo_plot,
    x="Periodo",
    y="Flujo Neto",
    color_discrete_sequence=[KAIROS_CAFE],
)
fig_flujo.update_layout(
    plot_bgcolor=KAIROS_BG,
    paper_bgcolor=KAIROS_BG,
    xaxis=dict(type="category", title=""),
    yaxis=dict(title=""),
)
st.plotly_chart(fig_flujo, use_container_width=True)

fig_acum = px.line(
    flujo_plot,
    x="Periodo",
    y="Flujo Acumulado",
    markers=True,
    color_discrete_sequence=[KAIROS_GOLD],
)
fig_acum.update_layout(
    plot_bgcolor=KAIROS_BG,
    paper_bgcolor=KAIROS_BG,
    xaxis=dict(type="category", title=""),
    yaxis=dict(title=""),
)
st.plotly_chart(fig_acum, use_container_width=True)

# ======================================================
# ESTADO DE RESULTADOS (FILTRO) - MÁS FINANCIERO
# Sin inversión
# ======================================================
st.divider()
st.subheader("Estado de Resultados (Filtro actual)")

estado = pd.DataFrame([
    ["Ventas", ventas_total],
    ["(-) Costos Variables", -costos_variables],
    ["(-) Costos Fijos", -costos_fijos],
    ["= EBIT", ebit],
    ["= EBITDA", ebitda],
    ["(-) Impuestos", -impuestos],
    ["= Resultado Neto", resultado_neto],
], columns=["Concepto", "Monto"])

# Formatear dinero
estado_fmt = estado.copy()
estado_fmt["Monto"] = estado_fmt["Monto"].map(fmt_money)

# Función de estilo
def style_estado(row):
    styles = []
    for col in estado_fmt.columns:
        if col == "Monto":
            # Negativos en rojo
            if "-" in str(row[col]):
                base_style = "color:red;"
            else:
                base_style = "color:black;"
            
            # Resaltar EBIT y Resultado Neto
            if row["Concepto"] in ["= EBIT", "= Resultado Neto"]:
                base_style += " font-weight:700;"
            
            styles.append(base_style)
        else:
            # Resaltar conceptos clave
            if row["Concepto"] in ["= EBIT", "= Resultado Neto"]:
                styles.append("font-weight:700;")
            else:
                styles.append("")
    return styles

styled_estado = estado_fmt.style.apply(style_estado, axis=1)

st.dataframe(
    styled_estado,
    use_container_width=True
)

# ======================================================
# ESTADO DE RESULTADOS HISTÓRICO (NO FILTRADO)
# Meses en columnas + Inversión incluida (primera fila conceptual)
# ======================================================
st.divider()
st.subheader("Estado de Resultados Histórico (Mensual)")

# periodos presentes (ordenados)
periodos = sorted(pd.concat([ventas["fecha"], gastos["fecha"]]).dropna().dt.to_period("M").unique())

def sum_by_period(df: pd.DataFrame, period, mask=None, col="total"):
    if df.empty:
        return 0.0
    tmp = df[df["fecha"].dt.to_period("M") == period]
    if mask is not None:
        tmp = tmp[mask(tmp)]
    return float(tmp[col].sum()) if col in tmp.columns else 0.0

rows = {
    "Inversión (Implementación)": [],
    "Ventas": [],
    "Costos Variables": [],
    "Costos Fijos": [],
    "EBIT": [],
    "EBITDA": [],
    "Impuestos": [],
    "Resultado Neto": [],
}

for p in periodos:
    v = sum_by_period(ventas, p, col="total")

    if "grupo_3" in gastos.columns:
        inv = sum_by_period(gastos, p, mask=lambda d: d["grupo_3"] == "INVERSION", col="total")
        cv = sum_by_period(gastos, p, mask=lambda d: d["grupo_3"] == "COSTO VARIABLE", col="total")
        cf = sum_by_period(gastos, p, mask=lambda d: d["grupo_3"] == "COSTO FIJO", col="total")
    else:
        inv = 0.0
        cv = sum_by_period(gastos, p, col="total")
        cf = 0.0

    e = v - cv - cf
    eda = e
    tax = (e * TAX_RATE) if e > 0 else 0.0
    net = e - tax

    rows["Inversión (Implementación)"].append(inv)
    rows["Ventas"].append(v)
    rows["Costos Variables"].append(-cv)
    rows["Costos Fijos"].append(-cf)
    rows["EBIT"].append(e)
    rows["EBITDA"].append(eda)
    rows["Impuestos"].append(-tax)
    rows["Resultado Neto"].append(net)

# construimos tabla tipo P&L con meses en columnas (labels cortos en español)
cols_labels = [period_label_from_period(p) for p in periodos]

hist_pl = pd.DataFrame(rows, index=cols_labels).T
hist_pl["Total"] = hist_pl.sum(axis=1)

# Formato dinero
hist_pl_fmt = hist_pl.copy()
for c in hist_pl_fmt.columns:
    hist_pl_fmt[c] = hist_pl_fmt[c].apply(lambda x: f"${x:,.0f}".replace(",", ".") if pd.notna(x) else "$0")

# Para poder pintar “primera columna” beige, convertimos índice a columna
hist_pl_fmt2 = hist_pl_fmt.reset_index().rename(columns={"index": "Concepto"})

sty_hist = (
    hist_pl_fmt2.style
    .hide(axis="index")   # elimina índice REAL
    .set_table_styles([
        {
            "selector": "th",
            "props": [
                ("background-color", KAIROS_CAFE),
                ("color", "white"),
                ("text-align", "center"),
                ("font-weight", "700"),
            ],
        },
        {
            "selector": "td",
            "props": [
                ("text-align", "center"),
            ],
        },
    ])
    .set_properties(
        subset=["Concepto"],
        **{
            "background-color": KAIROS_BEIGE,
            "color": KAIROS_CAFE,
            "font-weight": "700",
            "text-align": "left",
        },
    )
)
st.markdown(sty_hist.to_html(), unsafe_allow_html=True)



# ======================================================
# Break-even dinámico / PUNTO DE EQUILIBRIO
# ======================================================

st.divider()
st.subheader("Análisis Punto de Equilibrio")

if ventas_total > 0 and costos_variables > 0:
    margen_contribucion = 1 - (costos_variables / ventas_total)
else:
    margen_contribucion = 0

if margen_contribucion > 0:
    punto_equilibrio = costos_fijos / margen_contribucion
else:
    punto_equilibrio = 0

col_b1, col_b2 = st.columns(2)

col_b1.metric("Margen de Contribución", f"{margen_contribucion:.1%}")
col_b2.metric("Ventas necesarias para cubrir CF", fmt_money(punto_equilibrio))

# ======================================================
# EXPORTAR PDF
# ======================================================
#st.divider()
#st.subheader("Exportar Reporte Ejecutivo")

#if years_sel and months_sel:
#    filtros_label = (
#        f"Período: {min(years_sel)}-{max(years_sel)} | "
#        f"Meses: {', '.join([MESES[m] for m in months_sel])}"
#    )
#else:
#    filtros_label = "Período completo disponible"
#
#if st.button("📄 Generar PDF Profesional", use_container_width=True):

#    try:
#        with st.spinner("Generando reporte ejecutivo..."):
#
#            pdf_file = generar_pdf_html(
#                ventas_total=ventas_total,
#                costos_variables=costos_variables,
#                costos_fijos=costos_fijos,
#                ebit=ebit,
#                impuestos=impuestos,
#                resultado_neto=resultado_neto,
#                margen_operacional=margen_operacional,
#                punto_equilibrio=punto_equilibrio,
#                fig_resultado=fig_res,
#                fig_flujo=fig_flujo,
#                fig_acum=fig_acum,
#                hist_pl=hist_pl_fmt,
#                estado_df=estado.assign(Monto=estado["Monto"].map(fmt_money)),
#                filtros_label=filtros_label
#            )
#
#        with open(pdf_file, "rb") as pdf_bytes:
#            st.download_button(
#                label="⬇ Descargar PDF",
##                data=pdf_bytes,
#                file_name="Reporte_Ejecutivo_Kairos.pdf",
#                mime="application/pdf",
#                use_container_width=True
#            )
#
#    except Exception as e:
#        st.error("Error al generar PDF. Verifica instalación de wkhtmltopdf.")
#        st.exception(e)
# ======================================================
# DESCARGA COMPLETA DATA FILTRADA
# ======================================================

st.divider()
st.subheader("Descargar Data Completa (Filtro actual)")

import io

if st.button("📊 Descargar Excel Completo (Filtro actual)", use_container_width=True):

    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

        # Ventas filtradas
        if not ventas_f.empty:
            ventas_f.to_excel(writer, sheet_name="Ventas_Filtradas", index=False)

        # Gastos filtrados
        if not gastos_f.empty:
            gastos_f.to_excel(writer, sheet_name="Gastos_Filtrados", index=False)

        # Items filtrados
        if not items_f.empty:
            items_f.to_excel(writer, sheet_name="Items_Filtrados", index=False)

        # Estado de resultados filtro
        estado.to_excel(writer, sheet_name="Estado_Resultados", index=False)

    st.download_button(
        label="⬇ Descargar Excel",
        data=output.getvalue(),
        file_name="Kairos_Data_Filtrada.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )