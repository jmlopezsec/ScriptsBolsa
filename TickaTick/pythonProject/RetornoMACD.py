import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ============================================================
# 1. DESCARGA DE DATOS
# ============================================================

#inicialización
# Obtener los datos históricos de precios de una acción utilizando yfinance
ACCION= 'PYPL'
FechaIni= '2020-01-01'
FechaFin= '2024-11-21'
data = yf.download(ACCION, start=FechaIni, end=FechaFin, auto_adjust=False)


# ============================================================
# 2. CALCULAR MACD (12, 26, 9)
# ============================================================

# Calculo macd with standard colors first
# Calculo con precio de cierre
#macd = data.Close.ewm(span=12).mean() - data.Close.ewm(span=26).mean()
# Calculo con precio de cierre ajustado
macd = data['Adj Close'].ewm(span=12).mean() - data['Adj Close'].ewm(span=26).mean()

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
# 3. DETECTAR CRUCES MACD–SIGNAL
# ============================================================

data["Hist_prev"] = data["Histograma"].shift(1)

# Cruce HACIA ABAJO (Hist pasa de positivo → negativo)
data["DownCross"] = (data["Hist_prev"] > 0) & (data["Histograma"] < 0)

# Cruce HACIA ARRIBA (Hist pasa de negativo → positivo)
data["UpCross"] = (data["Hist_prev"] < 0) & (data["Histograma"] > 0)

down_dates = data.index[data["DownCross"]]
up_dates = data.index[data["UpCross"]]

print("Cruces hacia abajo:", len(down_dates))
#print("Cruces hacia arriba:", len(up_dates))
print(down_dates)


df=data
# ============================================================
# 4. CALCULAR RETORNOS A 1–30 DÍAS DESPUÉS DEL CRUCE
# ============================================================

DAYS = 30
returns_down = []

for date in down_dates:
    if date not in df.index:
        continue

    start_price = df.loc[date, "Adj Close"]

    # Para cada día 1..30 después del cruce
    ret_list = []
    for d in range(1, DAYS + 1):
        idx = df.index.get_indexer([date])[0]  # posición del índice
        if idx + d < len(df):
            future_price = df.iloc[idx + d]["Adj Close"]
            ret = (future_price - start_price) / start_price  # retorno en tanto por uno
            ret_list.append(ret)
        else:
            ret_list.append(np.nan)

    returns_down.append(ret_list)

returns_down = pd.DataFrame(returns_down, columns=[f"D{d}" for d in range(1, DAYS + 1)])
returns_down = returns_down.dropna()  # elimina filas incompletas

#Limpiar los elementos del dfataframe para quitar la parte literal.
returns_down = returns_down.applymap(lambda x: float(x.iloc[0]) if hasattr(x, "iloc") else float("nan"))


# ============================================================
# 5A. GRÁFICA 1 → NUBE DE PUNTOS (Días vs retorno)
# ============================================================

plt.figure(figsize=(12, 6))

for day in range(1, DAYS + 1):
    plt.scatter([day] * len(returns_down), returns_down[f"D{day}"], alpha=0.5)
    # Línea de mediana
    mediana = returns_down[f"D{day}"].median()
    plt.hlines(mediana, day - 0.2, day + 0.2, linewidth=3, color='black')
    # Línea de medis
    media = returns_down[f"D{day}"].mean()
    plt.hlines(media, day - 0.2, day + 0.2, linewidth=3, color='yellow')



plt.title(f"{ACCION} - Retornos tras cruce hacia ABAJO del MACD")
plt.xlabel("Días después del cruce")
plt.ylabel("Retorno (tanto por uno)")
plt.grid(True)
plt.show()

# ============================================================
# 5B. GRÁFICA 2 → HISTOGRAMA (bins de 0.5)
# ============================================================

plt.figure(figsize=(12, 6))

all_returns = returns_down.values.flatten()
all_returns = all_returns[~np.isnan(all_returns)]

# Bins de 0.5 (-10% .. +10%)
bins = np.arange(-0.5, 0.51, 0.05)  # Si quieres bins de EXACTO 0.5, cambia 0.05 -> 0.5

plt.hist(all_returns, bins=bins, edgecolor="black")

plt.title(f"{ACCION} - Distribución de retornos tras cruces hacia ABAJO del MACD")
plt.xlabel("Retorno (tanto por uno)")
plt.ylabel("Frecuencia")
plt.grid(True)
plt.show()
