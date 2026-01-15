
import streamlit as st
import pandas as pd
from pathlib import Path



st.set_page_config(page_title="Tabla editable", layout="wide")
#st.title("CONSOLA")

# Ruta del Excel
RUTA = Path("ibcopia.xlsx")

# Columnas editables y opciones
COLUMNAS_EDITABLES = ["Estado", "Bloque"]
OPCIONES_ESTADO = ["Abierta", "Cerrada", "Asignada", "Expirada"]

# --------- Carga / guardado ---------
def cargar_excel(path: Path) -> pd.DataFrame:
    # Recomendado: sin cache para evitar inconsistencias
    return pd.read_excel(path, engine="openpyxl")

def guardar_excel(path: Path, df: pd.DataFrame) -> None:
    df.to_excel(path, index=False, engine="openpyxl")

def fmt_moneda(v):
    return (f"{v:,.2f}" + "€").replace(",", "X").replace(".", ",").replace("X", ".")


def kpi_color(label, value, positivo):
    color = "#2ecc71" if positivo else "#e74c3c"
    st.markdown(
        f"""
        <div style="
            padding: 12px;
            border-radius: 8px;
            background-color: rgba(0,0,0,0.02);
            text-align: center;
        ">
            <div style="font-size:14px; color:#6c757d;">{label}</div>
            <div style="font-size:28px; font-weight:700; color:{color};">
                {fmt_moneda(value)}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


# --- Cargar datos ---
if not RUTA.exists():
    st.error(f"El fichero {RUTA.name} no existe en la misma carpeta que app.py")
    st.stop()

mtabla = cargar_excel(RUTA)

# Selección y renombre para la vista
df = mtabla.loc[:, [
    "datetime", "symbol", "underlying_price", "side", "shares", "right",
    "expiry", "price", "commission", "gross_value", "Estado", "Bloque"
]].copy()

df = df.rename(columns={
    "datetime": "Fecha",
    "symbol": "Valor",
    "underlying_price": "Cotizacion",
    "shares": "Posicion",
    "right": "Tipo",
    "expiry": "Expiracion",
    "commission": "Comision",
    "gross_value": "Total"
})

# =========================
#      FILTROS (SIDEBAR)
# =========================
with st.sidebar:
    st.header("Filtros")

    cols = list(df.columns)

    COL_FILTRO_1 = cols[1] if len(cols) > 1 else cols[0]
    COL_FILTRO_2 = "Estado" if "Estado" in df.columns else cols[0]

    opciones_1 = ["(Todos)"] + sorted(df[COL_FILTRO_1].dropna().astype(str).unique())
    seleccion_1 = st.selectbox(f"Filtrar por {COL_FILTRO_1}", opciones_1)

    df_intermedio = df if seleccion_1 == "(Todos)" else df[df[COL_FILTRO_1].astype(str) == str(seleccion_1)]

    opciones_2 = ["(Todos)"] + sorted(df_intermedio[COL_FILTRO_2].dropna().astype(str).unique())
    seleccion_2 = st.selectbox(f"Filtrar por {COL_FILTRO_2}", opciones_2)

    df_filtrado = (
        df_intermedio
        if seleccion_2 == "(Todos)"
        else df_intermedio[df_intermedio[COL_FILTRO_2].astype(str) == str(seleccion_2)]
    )


# =========================
#      INDICADORES (KPIs)
# =========================

ESTADO_OBJETIVO = "Abierta"

def _to_numeric_safe(series):
    return pd.to_numeric(series, errors="coerce").fillna(0)

def _norm_estado(series):
    return series.astype(str).str.strip().str.casefold()

# --- KPI 1: Abiertas (sobre df_filtrado: respeta ambos filtros)
if {"Estado", "Total"}.issubset(df_filtrado.columns):
    mask_abiertas_filtrado = _norm_estado(df_filtrado["Estado"]) == ESTADO_OBJETIVO.casefold()
    total_abiertas_filtrado = _to_numeric_safe(df_filtrado.loc[mask_abiertas_filtrado, "Total"]).sum()
else:
    total_abiertas_filtrado = 0.0

# --- KPI 2 (NUEVA LÓGICA): No abiertas pero SOLO con filtro por Valor (df_intermedio)
#     -> Ignora el filtro por Estado
if {"Estado", "Total"}.issubset(df_intermedio.columns):
    mask_no_abiertas_valor = _norm_estado(df_intermedio["Estado"]) != ESTADO_OBJETIVO.casefold()
    total_no_abiertas_solo_valor = _to_numeric_safe(df_intermedio.loc[mask_no_abiertas_valor, "Total"]).sum()
else:
    total_no_abiertas_solo_valor = 0.0

# --- KPI 3: No abiertas (GLOBAL, sobre todo mtabla sin filtros)
if {"Estado", "gross_value"}.issubset(mtabla.columns):
    mask_no_abiertas_global = _norm_estado(mtabla["Estado"]) != ESTADO_OBJETIVO.casefold()
    total_no_abiertas_global = _to_numeric_safe(mtabla.loc[mask_no_abiertas_global, "gross_value"]).sum()
else:
    total_no_abiertas_global = 0.0

# =========================
#      HEADER (TÍTULO + KPI)
# =========================
col_titulo, col_kpi = st.columns([4, 1])

with col_titulo:
    st.title("CONSOLA")

#with col_kpi:
#    st.metric(
#        label="TOTAL",
#        value=fmt_moneda(total_no_abiertas_global),
#        help="Ganancias acumuladas en todos los valores."
#    )

with col_kpi:
    kpi_color(
        label="TOTAL",
        value=total_no_abiertas_global,
        positivo=total_no_abiertas_global >= 0
    )



# =========================
#      KPIs SECUNDARIOS
# =========================
kpi1, kpi2 = st.columns(2)

with kpi1:
    st.metric(
        label="Abiertas",
        value=fmt_moneda(total_abiertas_filtrado),
        help="Total posición activa (abierta)."
    )

with kpi2:
    st.metric(
        label="Acumulado en el Valor",
        value=fmt_moneda(total_no_abiertas_solo_valor),
        help="Suma acumulada de las operaciones cerradas en el valor seleccionado."
    )



# ---------- Configuración de columnas ----------
column_config = {}
for col in df_filtrado.columns:
    if col not in COLUMNAS_EDITABLES:
        column_config[col] = st.column_config.Column(disabled=True)
    else:
        if col == "Estado":
            column_config[col] = st.column_config.SelectboxColumn(
                options=OPCIONES_ESTADO,
                help="Selecciona un estado",
                disabled=False
            )
        else:
            column_config[col] = st.column_config.Column(disabled=False)

# ---------- Tabla editable ----------
st.subheader("Tabla editable")
tabla_editada = st.data_editor(
    df_filtrado,
    num_rows="fixed",
    use_container_width=True,
    column_config=column_config,
    key="tabla_editable"
)

# ---------- Botones ----------
col1, col2 = st.columns(2)

with col1:
    if st.button("💾 Guardar cambios en Excel", use_container_width=True):
        try:
            # Tomamos SOLO cambios de columnas editables y los volcamos por índice
            cambios = tabla_editada[COLUMNAS_EDITABLES].copy()

            # Alinear por índice (el índice viene del Excel original)
            mtabla.loc[cambios.index, COLUMNAS_EDITABLES] = cambios

            # (Opcional) asegurar tipo object para evitar '<NA>' en Excel
            for c in COLUMNAS_EDITABLES:
                mtabla[c] = mtabla[c].astype(object)

            guardar_excel(RUTA, mtabla)

            # Si decidieras usar cache segura por mtime, aquí invalidas:
            # st.cache_data.clear()
            st.success(f"Cambios guardados en {RUTA.name}")
        except Exception as e:
            st.error(f"No se pudo guardar: {e}")

with col2:
    if st.button("🔄 Recargar desde Excel", use_container_width=True):
        # st.cache_data.clear()
        st.experimental_rerun()
