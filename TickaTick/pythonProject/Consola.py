
import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Tabla editable", layout="wide")
st.title("CONSOLA")

# Ruta del Excel
RUTA = Path("ibcopia.xlsx")


# === Configura aquí las columnas de filtro (o deja None para usar las 2 primeras) ===
COL_FILTRO_1 = None  # p. ej., "Producto"
COL_FILTRO_2 = None  # p. ej., "Categoría"


#----------------------------------------------
#       Selección de columnas editables


#----------------------------------------------


# === Seleccionar la columna editable ===
COLUMNAS_EDITABLES = ["Estado","Bloque"]

# === Opciones predefinidas ===
OPCIONES_ESTADO = ["Abierta", "Cerrada", "Asignada", "Expirada"]


# --- Funciones de carga y guardado ---
#@st.cache_data
def cargar_excel():
    return pd.read_excel(RUTA, engine="openpyxl")

def guardar_excel(df):
    df.to_excel(RUTA, index=False, engine="openpyxl")

# --- Cargar datos ---
if RUTA.exists():
    print("Leyendo del EXCEL")
    mtabla = cargar_excel()


    #mtabla= df[["datetime","symbol"]]
    df= mtabla[["datetime","symbol","underlying_price","side","shares","right","expiry","price","commission","gross_value","Estado","Bloque"]]



    df = df.rename(columns={
        "datetime": "Fecha",
        "symbol": "Valor",
        "underlying_price": "Cotizacion",
        "shares":"Posicion",
        "right":"Tipo",
        "expiry":"Expiracion",
        "commission":"Comision",
        "gross_value": "Total"
    })

    print("Creada tabla VISUAL")

else:
    st.error("El fichero datos.xlsx no existe en la misma carpeta que app.py")
    st.stop()

# -------------------------------------
#    SELECTOR ÚNICO (ejemplo por columna)
# -------------------------------------


# --- Resolver columnas de filtro por defecto si no están definidas ---
cols = list(df.columns)
if COL_FILTRO_1 is None:
    COL_FILTRO_1 = cols[1]
if COL_FILTRO_2 is None:
    # Usa la segunda columna si existe; si no, repite la primera
    COL_FILTRO_2 = cols[10] if len(cols) > 1 else cols[0]


# --- Construir opciones de los selectores ---
# Para una mejor UX, el segundo selector se recalcula tras aplicar el primero (filtros dependientes).
opciones_1 = ["(Todos)"] + sorted(df[COL_FILTRO_1].dropna().astype(str).unique().tolist())
seleccion_1 = st.selectbox(f"Filtrar por {COL_FILTRO_1}", opciones_1, index=0)

df_intermedio = df.copy()
if seleccion_1 != "(Todos)":
    # Compara como string para evitar problemas de tipos heterogéneos
    df_intermedio = df_intermedio[df_intermedio[COL_FILTRO_1].astype(str) == str(seleccion_1)]

opciones_2 = ["(Todos)"] + sorted(df_intermedio[COL_FILTRO_2].dropna().astype(str).unique().tolist())
seleccion_2 = st.selectbox(f"Filtrar por {COL_FILTRO_2}", opciones_2, index=0)

# --- Aplicar filtros combinados ---
df_filtrado = df_intermedio.copy()
if seleccion_2 != "(Todos)":
    df_filtrado = df_filtrado[df_filtrado[COL_FILTRO_2].astype(str) == str(seleccion_2)]

df_filtrado["Estado"] = df_filtrado["Estado"].astype("string")


# === Configuración de columnas ===
column_config = {}

for col in df_filtrado.columns:
    if col not in COLUMNAS_EDITABLES:
        column_config[col] = st.column_config.Column(disabled=True)
    else:
        if col == COLUMNAS_EDITABLES[0]:
            # Solo la primera editable como texto libre
            column_config[col] = st.column_config.SelectboxColumn(
                options=OPCIONES_ESTADO,
                help="Selecciona un estado",
                disabled=False
            )
        else:
            # Las demás editables con tipo genérico (o NumberColumn si quieres forzar numérico)
            column_config[col] = st.column_config.Column(
                disabled=False
            )
            # Si quieres forzar numérico y límites, usa:
            # column_config[col] = st.column_config.NumberColumn(min_value=0, step=1)



#'''---antes
#columna_selector = df.columns[1]     # Usamos la primera columna (puedes cambiarlo)
#opciones = sorted(df[columna_selector].unique())

#seleccion = st.selectbox(f"Filtrar por {columna_selector}", ["(Todos)"] + opciones)

#if seleccion != "(Todos)":
#    df_filtrado = df[df[columna_selector] == seleccion]
#else:
#    df_filtrado = df.copy()
#'''
# -------------------------------------
#    MOSTRAR TABLA EDITABLE
# -------------------------------------

st.subheader("Tabla editable")
# === Mostrar la tabla ===
tabla_editada = st.data_editor(
    df_filtrado,
    num_rows="fixed",
    use_container_width=True,
    column_config=column_config     #Añadida para bloquear
)

print ("VISUALIZAMOS TABLA")
# -------------------------------------
#    BOTONES: GUARDAR Y RECARGAR
# -------------------------------------

col1, col2 = st.columns(2)

# --- GUARDAR ---
with col1:
    if st.button("💾 Guardar cambios en Excel"):
        try:
            # Actualiza solo las filas filtradas

            df.update(tabla_editada)


            mtabla[["Estado", "Bloque"]] = df[["Estado", "Bloque"]]

            guardar_excel(mtabla)
            st.success("Cambios guardados en datos.xlsx")
            #st.cache_data.clear()
            print("Guardamos Excel")

        except Exception as e:
            st.error(f"No se pudo guardar: {e}")

# --- RECARGAR ---
with col2:
    if st.button("🔄 Recargar desde Excel"):
        st.cache_data.clear()
        st.experimental_rerun()
