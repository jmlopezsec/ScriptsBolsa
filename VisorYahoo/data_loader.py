# Se utiliza para descargar los datos desde yahoo con el periodo y duranción de datos.

import yfinance as yf
import pandas as pd
from config import DEFAULT_PERIOD, DEFAULT_INTERVAL

'''
def cargar_datos(tickers, periodo='6mo', intervalo='1d'):
    datos = {}

    for ticker in tickers:
        df = yf.download(ticker, period=periodo, interval=intervalo, auto_adjust=True)

        if df.empty:
            print(f"[⚠️] No se pudieron descargar datos para: {ticker}")
            continue

        # Establecer el índice como datetime
        df.index = pd.to_datetime(df.index)
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']]  # Asegurar columnas clave
        datos[ticker] = df

    return datos

'''



def cargar_datos(tickers, ticker_principal, mismos=[], periodo='6mo', intervalo='1d'):
    datos_dict = {}

    for ticker in tickers:
        df = yf.download(ticker, period=periodo, interval=intervalo)
        df.dropna(inplace=True)

        # Determinar si va en la misma ventana del principal
        misma_ventana = (ticker == ticker_principal) or (ticker in mismos)

        datos_dict[ticker] = {
            'df': df,  # <- renombramos la clave a 'df' para que lo entienda plotter.py
            'mismo': misma_ventana  # <- usamos la misma clave que espera graficar_activos
        }

    return datos_dict

