import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import timedelta

# -----------------------------
# Configuración general
# -----------------------------
st.set_page_config(
    page_title="Open Interest Explorer",
    layout="wide"
)

st.title("📊 Open Interest – Calls vs Puts")

# -----------------------------
# Carga de datos
# -----------------------------
@st.cache_data
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)

    df["date"] = pd.to_datetime(df["date"])
    df["inserted_at"] = pd.to_datetime(df["inserted_at"])
    df["expiry"] = pd.to_datetime(df["expiry"].astype(str), format="%Y%m%d")
    df["right"] = df["right"].str.upper()

    # Asegurar numéricos
    df["strike"] = pd.to_numeric(df["strike"])
    df["open_interest"] = pd.to_numeric(df["open_interest"]).fillna(0)

    return df


DATA_PATH = "open_interest.xlsx"
df = load_data(DATA_PATH)

# -----------------------------
# Sidebar – Filtros
# -----------------------------
st.sidebar.header("🔍 Filtros")

symbol = st.sidebar.selectbox(
    "Símbolo",
    sorted(df["symbol"].unique())
)

df_symbol = df[df["symbol"] == symbol]

expiry = st.sidebar.selectbox(
    "Vencimiento",
    sorted(df_symbol["expiry"].unique()),
    format_func=lambda x: x.strftime("%Y-%m-%d")
)

df_expiry = df_symbol[df_symbol["expiry"] == expiry]

date_selected = st.sidebar.selectbox(
    "Fecha",
    sorted(df_expiry["date"].dt.date.unique())
)

# 🔥 NUEVO: ventana temporal
window_days = st.sidebar.slider(
    "Ventana temporal (días hacia atrás)",
    min_value=1,
    max_value=10,
    value=1
)

# -----------------------------
# Filtrado temporal
# -----------------------------

end_date = date_selected
start_date = end_date - timedelta(days=window_days - 1)

df_window = df_expiry[
    (df_expiry["date"].dt.date >= start_date) &
    (df_expiry["date"].dt.date <= end_date)
]



# -----------------------------
# Preparación para el gráfico
# -----------------------------
# Agregamos por strike + right
df_agg = (
    df_window
    .groupby(["strike", "right"], as_index=False)
    .agg({"open_interest": "sum"})
)

# Puts en negativo
df_agg.loc[df_agg["right"] == "P", "open_interest"] *= -1

# Orden por strike
df_agg = df_agg.sort_values("strike")

# Separar Calls y Puts
calls = df_agg[df_agg["right"] == "C"]
puts = df_agg[df_agg["right"] == "P"]

# -----------------------------
# Gráfico
# -----------------------------
fig = go.Figure()

fig.add_bar(
    x=calls["strike"],
    y=calls["open_interest"],
    name="Calls",
)

fig.add_bar(
    x=puts["strike"],
    y=puts["open_interest"],
    name="Puts",
)

fig.update_layout(
    title=f"""
    Open Interest – {symbol}  
    Vencimiento {expiry.strftime('%Y-%m-%d')} | 
    {window_days} día(s) hasta {end_date}

    """,
    xaxis_title="Strike",
    yaxis_title="Open Interest",
    barmode="relative",
    height=600,
)

st.plotly_chart(fig, use_container_width=True)

# -----------------------------
# Debug opcional
# -----------------------------
with st.expander("🔎 Datos agregados (debug)"):
    st.dataframe(df_agg, use_container_width=True)
