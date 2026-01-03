import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#import talib as ta
from scipy.stats import linregress
from sklearn.linear_model import LinearRegression
import pandas_ta as ta



# ============================================================
# 1. DESCARGA DE DATOS
# ============================================================

#inicialización
# Obtener los datos históricos de precios de una acción utilizando yfinance
ACCION= 'PYPL'
FechaIni= '2016-01-01'
FechaFin= '2025-12-05'
data = yf.download(ACCION, start=FechaIni, end=FechaFin, auto_adjust=False)

# Aplana las columnas usando solo el primer nivel
data.columns = data.columns.get_level_values(0)


# ======================================================
# 2.    Ajusto cierres y valores
# ======================================================
# data contiene: Open, High, Low, Close, Adj Close, Volume
factor = data['Adj Close'] / data['Close']

data['Open'] = data['Open'] * factor
data['High'] = data['High'] * factor
data['Low'] = data['Low'] * factor
data['Close'] = data['Close'] * factor


# ============================================================
# 3. CALCULAR MACD (12, 26, 9)
# ============================================================

# Calculo macd with standard colors first
# Calculo con precio de cierre
#macd = data.Close.ewm(span=12).mean() - data.Close.ewm(span=26).mean()
# Calculo con precio de cierre ajustado
macd = data['Close'].ewm(span=12).mean() - data['Close'].ewm(span=26).mean()

signal = macd.ewm(span=9).mean()
histogram = macd - signal

# Agregar el MACD al dataframe
data['MACD'] = macd
data['Signal'] = signal
data['Histograma']= histogram

#Elimino los primeros 35 elementos del MACD poniendolos a NaN
#Pensar en hacerlo versátil al cambiar los valores del MACD (12, 26, 9)


cols = ['MACD', 'Signal', 'Histograma']
first35_idx = data.index[:35]   # obtén las 35 primeras etiquetas del índice
data.loc[first35_idx, cols] = np.nan

# ============================================================
# 4. Calculamos el SAR utilizando talib
# ============================================================


df= pd.DataFrame()
df= data.ta.psar(af=0.02, af0=0.02, max_af=0.2, append=False)



#==============================================================================
# 5. Hay que limpiar el resultado del SAR y combinar dos columnas con el SAR
#==============================================================================

# Eliminar columnas en posiciones 2 y 3
cols_to_drop = [df.columns[i] for i in [2, 3] if i < len(df.columns)]
df = df.drop(columns=cols_to_drop)

#cambio los nombres de las columnas para facilitar su acceso luego
df = df.rename(columns={df.columns[0]: "SAR_L", df.columns[1]: "SAR_C"})

#===================  Inbtroducimos el SAR en el dataframe data
#data["SAR"] = df["PSAR_merged"]
data = pd.concat([data, df], axis=1)



#============== Esta función determina en función de las medias el estado del mercado: Alcista, bajista o neutral

def aplicar_estado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade las medias móviles simples (10, 50, 200) y una columna 'Estado'
    con valores 'alcista', 'neutral' o 'bajista' según la lógica:
      - 'alcista' si SMA10 > SMA50 > SMA200
      - 'bajista' si SMA10 < SMA50 < SMA200
      - 'neutral' si (SMA10 < SMA50 > SMA200) o (SMA10 > SMA50 > SMA200)
    """

    # Añadir medias móviles simples
    df['SMA10']  = df['Close'].rolling(window=10).mean()
    df['SMA50']  = df['Close'].rolling(window=50).mean()
    df['SMA200'] = df['Close'].rolling(window=200).mean()

    # Condiciones
    cond_alcista = (df['SMA10'] > df['SMA50']) & (df['SMA50'] > df['SMA200'])
    cond_bajista = (df['SMA10'] < df['SMA50']) & (df['SMA50'] < df['SMA200'])
    #cond_neutral = not (cond_alcista | cond_bajista)

    # Asignar estados
    df['Estado'] = 'neutral'
    df.loc[cond_alcista, 'Estado'] = 'alcista'
    df.loc[cond_bajista, 'Estado'] = 'bajista'
    #df.loc[cond_neutral, 'Estado'] = 'neutral'

    return df


#Se determina el estado del mercado
data = aplicar_estado(data)


# ============================================================
# 6.   DETECTAR CRUCES MACD–SIGNAL
# ============================================================

data["Hist_prev"] = data["Histograma"].shift(1)

# Cruce HACIA ABAJO (Hist pasa de positivo → negativo)
data["DownCross"] = (data["Hist_prev"] > 0) & (data["Histograma"] < 0)

# Cruce HACIA ARRIBA (Hist pasa de negativo → positivo)
data["UpCross"] = (data["Hist_prev"] < 0) & (data["Histograma"] > 0)

down_dates = data.index[data["DownCross"]]
up_dates = data.index[data["UpCross"]]

print("Cruces hacia abajo:", len(down_dates))
print("Cruces hacia arriba:", len(up_dates))
#print(down_dates)


#================= Esta función genera dos columnas corto y largo y marca como están las entradas
#================= Puede ser reposo, medio si se dan dos indicadores o fuerte cuando los tres están alineados

def marcar_entradas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade columnas 'Corto' y 'Largo' al DataFrame con valores:
      - 'reposo' por defecto
      - 'medio' si se cumplen las condiciones básicas
      - 'fuerte' si además el Estado confirma la tendencia
    """

    # --- Filtro: filas con las tres SM válidas (no NaN) ---
    valid_sm = df[['SMA10', 'SMA50', 'SMA200']].notna().all(axis=1)

    # Inicializar columnas
    df['Corto'] = 'reposo'
    df['Largo'] = 'reposo'

    # --- Condiciones para Corto ---
    cond_corto_medio = (df['DownCross'] == True) & (df['SAR_C'] > df['High'])
    cond_corto_fuerte = cond_corto_medio & (df['Estado'] == 'bajista')

    df.loc[cond_corto_medio  & valid_sm, 'Corto'] = 'medio'
    df.loc[cond_corto_fuerte & valid_sm, 'Corto'] = 'fuerte'

    # --- Condiciones para Largo ---
    cond_largo_medio = (df['UpCross'] == True) & (df['SAR_L'] < df['Low'])
    cond_largo_fuerte = cond_largo_medio & (df['Estado'] == 'alcista')

    df.loc[cond_largo_medio  & valid_sm, 'Largo'] = 'medio'
    df.loc[cond_largo_fuerte & valid_sm, 'Largo'] = 'fuerte'

    return df


#===== marca el tipo d eentrada e intensidad
data = marcar_entradas(data)


def valor_inicio_bloque(df, col, i0):
    s = df[col]
    # Máscara de "es numérico (no NaN)"
    mask = s.notna()

    # Si estás en un NaN, no hay bloque numérico
    if not mask.iloc[i0]:
        return None

    # Crear IDs de bloques alternando NaN / numérico
    # Cada vez que cambia mask (False→True o True→False), se inicia un bloque nuevo
    bloques = mask.ne(mask.shift()).cumsum()

    # ID del bloque actual
    g = bloques.iloc[i0]

    # Índice de inicio del bloque actual (primer índice del grupo g)
    inicio_idx = df.index[bloques.eq(g)][0]

    # Valor numérico en el inicio del bloque
    return s.loc[inicio_idx]



def estadisticas_largo(df: pd.DataFrame, dias: int = 30) -> pd.DataFrame:
    """
    Para cada fila donde Largo == 'medio', genera un DataFrame con tantas filas como señales
    y columnas Day1...DayN con True/False según si Low >= Open*0.95 en los días posteriores.
    Maneja índices temporales usando posiciones (iloc).
    """

    # Asegurar que existen las columnas requeridas
    required_cols = {'Largo', 'Open', 'Low'}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    # Filtrar señales "Largo medio" y obtener posiciones de fila (enteros)
    señales_idx = df.index[df['Largo'] == 'medio']          # Índices (pueden ser fechas)
    señales_pos = [df.index.get_loc(i) for i in señales_idx]  # Posiciones enteras

    resultados = []

    for pos, idx in zip(señales_pos, señales_idx):
        # open_signal = df.iloc[pos]['Open']
        # umbral = open_signal * 0.95
        open_signal = df.iloc[pos]['SAR_L']
        umbral = valor_inicio_bloque(df, 'SAR_L', pos)


        fila_resultado = {
            'signal_index': idx,        # índice original (fecha si DatetimeIndex)
            'signal_open': open_signal,
            'threshold': umbral
        }

        # Para cada día posterior por posición
        for d in range(1, dias + 1):
            target_pos = pos + d
            if target_pos < len(df):
                low_val = df.iloc[target_pos]['Low']
                fila_resultado[f'Day{d}'] = low_val >= umbral
            else:
                fila_resultado[f'Day{d}'] = None  # fuera de rango

        resultados.append(fila_resultado)

    # Construir DataFrame de resultados con índice como la fecha (o índice original) de la señal
    if resultados:
        resultados_df = pd.DataFrame(resultados).set_index('signal_index')
    else:
        # Si no hay señales, devuelve DataFrame vacío con las columnas esperadas
        cols = ['signal_open', 'threshold'] + [f'Day{d}' for d in range(1, dias + 1)]
        resultados_df = pd.DataFrame(columns=cols)

    return resultados_df


res = estadisticas_largo(data, dias=30)


# --- 0) Identificar las columnas de días ---
# Si las columnas de días empiezan en la tercera columna (índice de columna 2):
day_cols = res.columns[2:]  # toma desde la 3ª columna hasta el final (las 30 de días)

# Si prefieres asegurar que solo coges booleanas:
# day_cols = res.columns[2:][res.dtypes.iloc[2:] == bool]

# --- 1) Asegura tipos y orden ---
# Convierte las columnas de días a booleano (por si hubiese 0/1 o strings)
res[day_cols] = res[day_cols].astype(bool)

# Si necesitas ordenar las columnas (por ejemplo 'd1'...'d30'):
# day_cols = sorted(day_cols, key=lambda c: int(''.join(ch for ch in c if ch.isdigit()) or 0))

# --- 2) Cálculo vectorizado del primer False por fila ---
arr = (~res[day_cols]).to_numpy()      # True donde el original es False
has_false = arr.any(axis=1)            # ¿hay algún False en la fila?
first_false_pos = arr.argmax(axis=1)   # posición 0..n-1 del primer False


# Corrige el caso "no hay False": argmax devuelve 0 → lo sustituimos por NaN
first_false_pos = np.where(has_false, first_false_pos, np.nan)

# --- 3B) Si quieres el número de día (entero) ---
# Si las etiquetas son 'd1','d2',...,'d30', extraemos el número:
def label_to_day(c):
    try:
        return int(''.join(ch for ch in c if ch.isdigit()))
    except Exception:
        return np.nan


res['DiasRotura'] = first_false_pos

print (data)



def estadisticas_entradas(df: pd.DataFrame, tipo: str, matiz: str, dias: int = 30) -> pd.DataFrame:
    """
    Genera, para cada fila donde {tipo} == {matiz}, un DataFrame con tantas filas como señales y
    columnas Day1...DayN con True/False en función de:
      - Si tipo == 'Largo':  Low(día + d)  >= umbral
      - Si tipo == 'Corto':  High(día + d) <= umbral
    El umbral se obtiene como valor de inicio de bloque del SAR correspondiente en la posición de la señal:
      - Largo -> 'SAR_L'
      - Corto -> 'SAR_C'

    Parámetros
    ----------
    df : pd.DataFrame
        DataFrame con, al menos, las columnas:
          'Largo', 'Corto', 'Open', 'Low', 'High', 'SAR_L', 'SAR_C'
    tipo : {'Largo','Corto'}
        Tipo de entrada a analizar.
    matiz : {'medio','fuerte'}
        Matiz de la entrada.
    dias : int
        Número de días posteriores a evaluar (columnas Day1..DayN).

    Returns
    -------
    pd.DataFrame
        Índice = índice original de la señal (por ejemplo, fecha).
        Columnas: ['signal_open', 'threshold', 'tipo', 'matiz', 'sar_col', 'precio_col', 'cmp'] + Day1..DayN
    """

    # --- Validaciones de parámetros ---
    tipo = str(tipo).strip().capitalize()   # normaliza 'largo'/'corto' -> 'Largo'/'Corto'
    matiz = str(matiz).strip().lower()      # normaliza 'Medio'/'Fuerte' -> 'medio'/'fuerte'

    if tipo not in {'Largo', 'Corto'}:
        raise ValueError("Parametro 'tipo' debe ser 'Largo' o 'Corto'.")
    if matiz not in {'medio', 'fuerte'}:
        raise ValueError("Parametro 'matiz' debe ser 'medio' o 'fuerte'.")
    if dias <= 0:
        raise ValueError("Parametro 'dias' debe ser un entero positivo.")

    # --- Columnas requeridas según el tipo ---
    # Comunes
    required = {'Open', 'Low', 'High', 'Largo', 'Corto', 'SAR_L', 'SAR_C'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    # --- Configuración por tipo ---
    if tipo == 'Largo':
        sar_col = 'SAR_L'
        precio_col = 'Low'
        # Comparación: Low >= umbral  -> True
        def compara(valor, umbral): return valor >= umbral
        cmp_desc = f"{precio_col} >= threshold"
    else:  # 'Corto'
        sar_col = 'SAR_C'
        precio_col = 'High'
        # Comparación: High <= umbral -> True
        def compara(valor, umbral): return valor <= umbral
        cmp_desc = f"{precio_col} <= threshold"

    # --- Filtrado de señales {tipo}-{matiz} ---
    # Ej.: df['Largo'] == 'medio'  o  df['Corto'] == 'fuerte'
    señales_idx = df.index[df[tipo] == matiz]
    if len(señales_idx) == 0:
        # DataFrame vacío con la forma esperada
        cols = ['signal_open', 'threshold', 'tipo', 'matiz', 'sar_col', 'precio_col', 'cmp'] \
               + [f'Day{d}' for d in range(1, dias + 1)]
        return pd.DataFrame(columns=cols)

    # Convertir a posiciones enteras (por si índice temporal)
    señales_pos = [df.index.get_loc(i) for i in señales_idx]

    resultados = []

    for pos, idx in zip(señales_pos, señales_idx):
        # "open_signal" como el valor SAR en el momento de la señal (igual a tu código anterior)
        open_signal = df.iloc[pos][sar_col]

        # Umbral usando tu función de inicio de bloque sobre la misma columna SAR
        umbral = valor_inicio_bloque(df, sar_col, pos)

        fila_resultado = {
            'signal_index': idx,         # índice original (fecha si DatetimeIndex)
            'signal_open': open_signal,
            'threshold'  : umbral,
            'tipo'       : tipo,
            'matiz'      : matiz,
            'sar_col'    : sar_col,
            'precio_col' : precio_col,
            'cmp'        : cmp_desc
        }

        # Días posteriores por posición
        for d in range(1, dias + 1):
            target_pos = pos + d
            if target_pos < len(df):
                precio_val = df.iloc[target_pos][precio_col]
                # Si el umbral es NaN (p. ej., bloques no válidos), devuelve None
                if pd.isna(umbral) or pd.isna(precio_val):
                    fila_resultado[f'Day{d}'] = None
                else:
                    fila_resultado[f'Day{d}'] = compara(precio_val, umbral)
            else:
                fila_resultado[f'Day{d}'] = None  # fuera de rango

        resultados.append(fila_resultado)

    resultados_df = pd.DataFrame(resultados).set_index('signal_index')
    return resultados_df


res1= estadisticas_entradas(data, tipo='Largo', matiz='fuerte', dias=30)

# --- 0) Identificar las columnas de días ---
# Si las columnas de días empiezan en la tercera columna (índice de columna 2):
day_cols = res1.columns[7:]  # toma desde la 3ª columna hasta el final (las 30 de días)

# Si prefieres asegurar que solo coges booleanas:
# day_cols = res.columns[2:][res.dtypes.iloc[2:] == bool]

# --- 1) Asegura tipos y orden ---
# Convierte las columnas de días a booleano (por si hubiese 0/1 o strings)
res1[day_cols] = res1[day_cols].astype(bool)

# Si necesitas ordenar las columnas (por ejemplo 'd1'...'d30'):
# day_cols = sorted(day_cols, key=lambda c: int(''.join(ch for ch in c if ch.isdigit()) or 0))

# --- 2) Cálculo vectorizado del primer False por fila ---
arr = (~res1[day_cols]).to_numpy()      # True donde el original es False
has_false = arr.any(axis=1)            # ¿hay algún False en la fila?
first_false_pos = arr.argmax(axis=1)   # posición 0..n-1 del primer False

# Corrige el caso "no hay False": argmax devuelve 0 → lo sustituimos por NaN
first_false_pos = np.where(has_false, first_false_pos, np.nan)

# --- 3B) Si quieres el número de día (entero) ---
# Si las etiquetas son 'd1','d2',...,'d30', extraemos el número:
def label_to_day(c):
    try:
        return int(''.join(ch for ch in c if ch.isdigit()))
    except Exception:
        return np.nan


res1['DiasRotura'] = first_false_pos

res1['DiasRotura'] = np.where(res1['DiasRotura'].notna(),
                              res1['DiasRotura'] + 1,
                              res1['DiasRotura'])


# 1) Asegurar que la columna es numérica (lo que no sea número -> NaN)
s = pd.to_numeric(res1['DiasRotura'], errors='coerce')



# 2) Porcentaje de NaN
porc_nan = s.isna().mean() * 100

# 3) Filtrar solo valores numéricos (excluye NaN)
vals = s.dropna()

# 4) Estadísticos solicitados
media   = vals.mean()
mediana = vals.median()
std     = vals.std(ddof=1)  # desviación típica muestral
iqr     = vals.quantile(0.75) - vals.quantile(0.25)



resumen = pd.Series({
    'filas_totales'      : s.size,
    'n_nan'              : s.isna().sum(),
    'porc_nan_%'         : porc_nan,
    'n_valores_numericos': vals.size,
    'media'              : media,
    'mediana'            : mediana,
    'desv_tipica'        : std,
    'rango_intercuartil' : iqr,
})

print(resumen.round(1))