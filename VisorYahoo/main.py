
# main.py

from data_loader import cargar_datos
from indicadores import aplicar_indicadores
from plotter import graficar_activos

from config import DEFAULT_PERIOD, DEFAULT_INTERVAL


# Elegir tickers y periodo
tickers = ['INTC'] #Se deben incluir los tickes de los activos a descargar ['INTC', 'T', 'SBLK']
ticker_principal = 'INTC'
mismos = ['']  # 'T' se dibuja junto a 'INTC', 'SBLK' en ventana aparte


periodo = DEFAULT_PERIOD
intervalo = DEFAULT_INTERVAL

# Descargar datos

datos = cargar_datos(tickers, ticker_principal, mismos, periodo=periodo, intervalo=intervalo)

# Calcular indicadores y generar configuración de overlays
overlays = aplicar_indicadores(datos)

# Graficar
graficar_activos(datos, overlays, mismos)
