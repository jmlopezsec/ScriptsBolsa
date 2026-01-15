
import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Tabla editable", layout="wide")
st.title("CONSOLA")

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

# ---------- Filtros ----------
cols = list(df.columns)

# Primer filtro: por defecto la segunda columna visible
COL_FILTRO_1 = cols[1] if len(cols) > 1 else cols[0]

# Segundo filtro: usa "Estado" si existe; si no, una columna segura
COL_FILTRO_2 = "Estado" if "Estado" in df.columns else (cols[min(10, len(cols)-1)] if len(cols) > 1 else cols[0])

opciones_1 = ["(Todos)"] + sorted(df[COL_FILTRO_1].dropna().astype(str).unique().tolist())
seleccion_1 = st.selectbox(f"Filtrar por {COL_FILTRO_1}", opciones_1, index=0)

df_intermedio = df if seleccion_1 == "(Todos)" else df[df[COL_FILTRO_1].astype(str) == str(seleccion_1)]

opciones_2 = ["(Todos)"] + sorted(df_intermedio[COL_FILTRO_2].dropna().astype(str).unique().tolist())
seleccion_2 = st.selectbox(f"Filtrar por {COL_FILTRO_2}", opciones_2, index=0)

df_filtrado = df_intermedio if seleccion_2 == "(Todos)" else df_intermedio[df_intermedio[COL_FILTRO_2].astype(str) == str(seleccion_2)]



# =========================
#      INDICADORES (KPIs)
# =========================

# Normalizamos mayúsculas/minúsculas en Estado para comparar de forma robusta
ESTADO_OBJETIVO = "Abierta"


def _to_numeric_safe(series):
    return pd.to_numeric(series, errors="coerce").fillna(0)

# --- KPI 1: suma de "Total" en filtrado donde Estado == "Abierta"
df_filtrado_local = df_filtrado.copy()
# Asegurar que existen las columnas esperadas
if "Estado" in df_filtrado_local.columns and "Total" in df_filtrado_local.columns:
    mask_abiertas = df_filtrado_local["Estado"].astype(str).str.strip().str.casefold() == ESTADO_OBJETIVO.casefold()
    total_abiertas_filtrado = _to_numeric_safe(df_filtrado_local.loc[mask_abiertas, "Total"]).sum()
else:
    total_abiertas_filtrado = 0.0

# --- KPI 2: suma de "Total" en filtrado donde Estado != "Abierta"
if "Estado" in df_filtrado_local.columns and "Total" in df_filtrado_local.columns:
    mask_no_abiertas = df_filtrado_local["Estado"].astype(str).str.strip().str.casefold() != ESTADO_OBJETIVO.casefold()
    total_no_abiertas_filtrado = _to_numeric_safe(df_filtrado_local.loc[mask_no_abiertas, "Total"]).sum()
else:
    total_no_abiertas_filtrado = 0.0

# --- KPI 3 (global): suma de "gross_value" en TODO mtabla donde Estado != "Abierta"
mtabla_local = mtabla.copy()
if "Estado" in mtabla_local.columns and "gross_value" in mtabla_local.columns:
    mask_no_abiertas_global = mtabla_local["Estado"].astype(str).str.strip().str.casefold() != ESTADO_OBJETIVO.casefold()
    total_no_abiertas_global = _to_numeric_safe(mtabla_local.loc[mask_no_abiertas_global, "gross_value"]).sum()
else:
    total_no_abiertas_global = 0.0

# --- Presentación de KPIs ---
def fmt_moneda(v):
    # Ajusta a tu preferencia de formato; aquí miles con punto y 2 decimales
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

kpi1, kpi2, kpi3 = st.columns(3)
with kpi1:
    st.metric(
        label="Abiertas (filtrado)",
        value=fmt_moneda(total_abiertas_filtrado),
        help="Suma de 'Total' para Estado='Abierta' en la tabla actualmente filtrada."
    )
with kpi2:
    st.metric(
        label="No abiertas (filtrado)",
        value=fmt_moneda(total_no_abiertas_filtrado),
        help="Suma de 'Total' para Estado≠'Abierta' en la tabla actualmente filtrada."
    )
with kpi3:
    st.metric(
        label="No abiertas (global)",
        value=fmt_moneda(total_no_abiertas_global),
        help="Suma de 'gross_value' para Estado≠'Abierta' en todos los datos del Excel."
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
