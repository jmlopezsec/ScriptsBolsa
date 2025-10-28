import yfinance as yf
import numpy as np
import pandas as pd
from scipy.stats import norm, skew, kurtosis

# Para que los gráficos se vean en el notebook

import matplotlib.pyplot as plt


#inicialización
# Obtener los datos históricos de precios de una acción utilizando yfinance
ACCION= 'AAPL'
FechaIni= '2014-01-01'
FechaFin= '2025-08-25'
data = yf.download(ACCION, start=FechaIni, end=FechaFin, auto_adjust=False)

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

#%%
#Calculo el cruce, en función del cambio del signo de la columna Histograma, para ello desplazo la columna histograma una posición y relaizo l amultiplicación.
#Si el resultado es negativo indica que ha habido un cruce. Una vez hehco esto lo cambio para tener una lógica positiva

#Calculo el cruce, en función del cambio del signo de la columna Histograma, para ello desplazo la columna histograma una posición y relaizo l amultiplicación.
#Si el resultado es negativo indica que ha habido un cruce. Una vez hehco esto lo cambio para tener una lógica positiva

data['Cruce']= np.sign(data['Histograma'] * data['Histograma'].shift(1))


# Inicializamos la columna de salida
data['Estado'] = 'reposo'

# Condiciones para posiciones largas
cond_largo = (
    (data['Cruce'] == -1) &
    (data['Histograma'] > 0) &
    (data['MACD'] < 0) &
    (data['Signal'] < 0)
)

# Condiciones para posiciones cortas
cond_corto = (
    (data['Cruce'] == -1) &
    (data['Histograma'] < 0) &
    (data['MACD'] > 0) &
    (data['Signal'] > 0)
)

# Aplicamos las condiciones
data.loc[cond_largo, 'Estado'] = 'largo'
data.loc[cond_corto, 'Estado'] = 'corto'



#Voy a determinar cuales son los indices donde se abre posición.

indices = data.index[data['Estado'] != 'reposo']

# ---------------------------
# Cálculo de retornos diarios hasta 30 días
# ---------------------------

resultados = []
resumen = []

# Tomamos todos los cruces
for idx, row in data[data['Estado'] != 'reposo'].iterrows():
    # precio_inicio = row['Adj Close']

    estado = row['Estado']
    estado = estado.iloc[0] if isinstance(estado, pd.Series) else estado
    fecha_inicio = idx

    # Localizamos la posición del índice
    idx_pos = data.index.get_loc(idx)

    # Creamos la ventana de 31 días (día 0 + 30 siguientes)
    ventana_w = data.iloc[idx_pos: idx_pos + 31]
    ventana= ventana_w.iloc[1:]

    if len(ventana_w) < 2:
        continue  # si no hay suficientes días, saltamos

    # Retornos relativos día a día respecto al precio del cruce
    precio_inicios = ventana_w['Adj Close'].iloc[0]
    precio_inicio = precio_inicios.iloc[0]
    retornos = (ventana['Adj Close'].values - precio_inicio) / precio_inicio
    retornos = np.ravel(retornos)

    # Crear DataFrame temporal
    df_temp = pd.DataFrame({
        'Fecha_inicio': [fecha_inicio] * len(ventana),
        'Fecha': ventana.index,
        'Estado': [estado] * len(ventana),
        'Dia': np.arange(len(ventana)),
        'Retorno': retornos
    })

    resumen.append({
        'Fecha_inicio': fecha_inicio,
        'Estado': estado,
        'Media_retorno': np.mean(retornos),
        'Mediana_retorno': np.median(retornos),
        'Desviacion_tipica': np.std(retornos),
        'Max_excursion': np.max(retornos),
        'Min_excursion': np.min(retornos),
        'Percentil_90': np.percentile(retornos, 90)
    })

    resultados.append(df_temp)

    # Calcular estadísticas por cada cruce


# Concatenar todos los resultados
# M este funcionaba    retornos_30d = pd.concat(resultados, ignore_index=True)

# Unir todos los resultados
df_resultados = pd.concat(resultados, ignore_index=True)
df_resumen = pd.DataFrame(resumen)

# Agrupar por 'Estado' y calcular las medias y medianas deseadas
resumen_estado = df_resumen.groupby('Estado').agg({
    'Media_retorno': ['mean', 'median'],
    'Mediana_retorno': ['mean', 'median'],
    'Max_excursion': ['mean', 'median'],
    'Min_excursion': ['mean', 'median'],
    'Desviacion_tipica': ['mean', 'median']
})

# Opcional: mejorar la presentación de las columnas
resumen_estado.columns = [
    'Media(Media_retorno)', 'Mediana(Media_retorno)',
    'Media(Mediana_retorno)', 'Mediana(Mediana_retorno)',
    'Media(Max_excursion)', 'Mediana(Max_excursion)',
    'Media(Min_excursion)', 'Mediana(Min_excursion)',
    'Media(Desviacion_tipica)','Mediana(Desviacion_tipica)'
]

# Mostrar el resumen final
print(resumen_estado)

print(resumen_estado)

# Supongamos que ya tienes el DataFrame resumen_estado definido así:
# resumen_estado = df_resumen.groupby('Estado').agg({
#     'Media_retorno': ['mean', 'median'],
#     'Mediana_retorno': ['mean', 'median'],
#     'Max_excursion': ['mean', 'median'],
#     'Min_excursion': ['mean', 'median'],
#     'Desviacion_tipica': ['mean', 'median']
# })

# Aplanamos las columnas para acceder más fácil
#resumen_estado.columns = ['_'.join(col) for col in resumen_estado.columns]

# Mostramos el DataFrame con nombres simples
print("DataFrame agrupado:\n")
print(resumen_estado, "\n")

# Extraemos los valores deseados
valores = {
    'largo': {
        'Mediana': resumen_estado.loc['largo', 'Mediana(Media_retorno)'],
        'Desviacion': resumen_estado.loc['largo', 'Mediana(Desviacion_tipica)']
    },
    'corto': {
        'Mediana': resumen_estado.loc['corto', 'Mediana(Media_retorno)'],
        'Desviacion': resumen_estado.loc['corto', 'Mediana(Desviacion_tipica)']
    }
}

# Obtener el último valor de cotización
precio_actual = data["Adj Close"].iloc[-1]
precio_actual = precio_actual.values[0]

for estado in resumen_estado.index: # recorre 'largo' y 'corto'
    # Extraer valores de cada estado
    mediana_ret = resumen_estado.loc[estado, 'Mediana(Media_retorno)' ]
    mediana_std = resumen_estado.loc[estado, 'Mediana(Desviacion_tipica)']

    # Sumar las métricas
    if estado == 'largo':
        suma_metricas = mediana_ret - mediana_std
        resultado = precio_actual-(suma_metricas * precio_actual)
    else:
        suma_metricas = mediana_ret + mediana_std
        resultado = (suma_metricas * precio_actual) + precio_actual

    # Aplicar la fórmula
    #resultado = (suma_metricas * precio_actual) + precio_actual

    print(f"Estado: {estado.upper()}")
    print(f"  ▸ Mediana retorno: {mediana_ret:.6f}")
    print(f"  ▸ Mediana desviación típica: {mediana_std:.6f}")
    print(f"  ▸ Suma métricas: {suma_metricas:.6f}")
    print(f"  ▸ Proyección: {resultado:.2f}\n")

'''
#-------------- Estadísticas Globales --------------------------------
retornos_globales = df_resultados[['Retorno']]

media_global = retornos_globales.mean()
mediana_global = retornos_globales.median()
std_global = retornos_globales.std()
max_excursion = retornos_globales.max()
min_excursion = retornos_globales.min()
percentil_90 = np.percentile(retornos_globales, 90)
asimetria = skew(retornos_globales, nan_policy='omit')
curtosis_val = kurtosis(retornos_globales, nan_policy='omit')

# Separar positivos y negativos
retornos_pos = retornos_globales[retornos_globales > 0]
retornos_neg = retornos_globales[retornos_globales < 0]

media_pos = retornos_pos.mean()
mediana_pos = retornos_pos.median()
std_pos = retornos_pos.std()

media_neg = retornos_neg.mean()
mediana_neg = retornos_neg.median()
std_neg = retornos_neg.std()

# Mostrar resultados
print("\n===== ESTADÍSTICAS GLOBALES =====")
print(f"Media global: {media_global:.6f}")
print(f"Mediana global: {mediana_global:.6f}")
print(f"Desviación típica global: {std_global:.6f}")
print(f"Máxima excursión: {max_excursion:.6f}")
print(f"Mínima excursión: {min_excursion:.6f}")
print(f"Percentil 90: {percentil_90:.6f}")
print(f"Asimetría (Skew): {asimetria:.6f}")
print(f"Curtosis: {curtosis_val:.6f}")

print("\n===== RETORNOS POSITIVOS =====")
print(f"Media: {media_pos:.6f}")
print(f"Mediana: {mediana_pos:.6f}")
print(f"Desviación típica: {std_pos:.6f}")

print("\n===== RETORNOS NEGATIVOS =====")
print(f"Media: {media_neg:.6f}")
print(f"Mediana: {mediana_neg:.6f}")
print(f"Desviación típica: {std_neg:.6f}")

# --- TABLA RESUMEN DE CADA CRUCE ---
print("\n===== RESUMEN POR CRUCE =====")
#display(df_resumen.head(10))  # muestra las 10 primeras filas

# --- GRÁFICO DE RETORNOS (30 días tras cruce) ---
plt.figure(figsize=(10, 5))
for fecha in df_resultados['Fecha_inicio'].unique()[:5]:
    sub = df_resultados[df_resultados['Fecha_inicio'] == fecha]
    plt.plot(sub['Dia'], sub['Retorno'], label=f"{fecha.date()} ({sub['Estado'].iloc[0]})")

plt.xlabel("Días desde cruce")
plt.ylabel("Retorno relativo")
plt.title(f"Evolución de retornos durante 30 días ({ACCION})")
plt.legend()
plt.grid(True)
plt.show()

# --- HISTOGRAMA DE RETORNOS + GAUSSIANA ---
plt.figure(figsize=(10, 5))

# Histograma normalizado
n, bins, patches = plt.hist(retornos_globales, bins=100, density=True, alpha=0.6, color='steelblue', edgecolor='black')

# Ajuste de curva normal
xmin, xmax = plt.xlim()
x = np.linspace(xmin, xmax, 200)
p = norm.pdf(x, media_global, std_global)
plt.plot(x, p, 'r', linewidth=2, label='Curva Normal Ajustada')

plt.title(f"Distribución de retornos ({ACCION})")
plt.xlabel("Retorno")
plt.ylabel("Densidad de frecuencia")
plt.grid(True)
plt.legend()
plt.show()
'''
