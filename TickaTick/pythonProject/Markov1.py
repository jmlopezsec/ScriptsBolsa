
# -*- coding: utf-8 -*-
"""
Markov + Recomendación de deltas (Wheel / Calls cubiertos) con datos reales de Yahoo Finance
--------------------------------------------------------------------------------------------
• Descarga OHLC v2 de Yahoo para un ticker dado y un rango temporal.
• Etiqueta estados (Alcista/Neutral/Bajista) según desviación de una SMA.
• Estima matriz de transición (con suavizado opcional).
• Proyecta probabilidades a D+5, D+10, D+20 (configurable).
• Recomienda deltas de PUT y CALL cubierto en función de las probabilidades.

Requisitos: yfinance, pandas, numpy
Instalación: pip install yfinance
Autor: M365 Copilot
"""

import numpy as np
import pandas as pd
import datetime as dt

try:
    import yfinance as yf
except ImportError:
    raise ImportError("Falta yfinance. Instálalo con: pip install yfinance")

# ---------------------------
# Parámetros principales
# ---------------------------

TICKER = "WM"            # <-- Cambia aquí el valor (MCD, AAPL, SPY, etc.)
PERIODO = "5y"            # "1y", "2y", "5y", "max" ...
INTERVALO = "1d"          # "1d" recomendado para estados diarios
MA_WINDOW = 50            # ventana SMA para clasificar estados
BANDA_PCT = 0.01          # banda ±1% alrededor de la SMA para Neutral
SUAVIZADO_LAPLACE = 1.0   # suavizado (evitar ceros: 0.0, 0.5, 1.0)
HORIZONTES = [5, 10, 20]  # días (n pasos) para proyectar distribuciones

# ---------------------------
# Funciones auxiliares
# ---------------------------

def descargar_yahoo(ticker: str, period: str = "5y", interval: str = "1d") -> pd.DataFrame:
    """
    Descarga datos de Yahoo Finance usando yfinance.
    Devuelve un DataFrame con columnas ['Date','Close'] ordenado por fecha.
    """
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=True, progress=False)
    if df.empty:
        raise ValueError(f"No se pudieron descargar datos para {ticker}. Revisa el ticker o el rango.")
    df = df.reset_index()
    # En yfinance, la columna de fecha suele llamarse 'Date' o 'Datetime'
    date_col = "Date" if "Date" in df.columns else "Datetime"
    df = df[[date_col, "Close"]].rename(columns={date_col: "Date"})
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)
    return df


def descargar_yahoo_robusto(ticker: str, period: str = "5y", interval: str = "1d"):
    import yfinance as yf

    df = yf.download(ticker, period=period, interval=interval, auto_adjust=True, progress=False)
    if df.empty:
        raise ValueError(f"No se pudieron descargar datos para {ticker}. Revisa el ticker o el rango.")

    df = df.reset_index()  # 'Date' o 'Datetime'

    # Normaliza nombre de fecha
    date_col = "Date" if "Date" in df.columns else ("Datetime" if "Datetime" in df.columns else None)
    if date_col is None:
        raise ValueError("No se encontró columna de fecha en el DataFrame descargado.")

    # Si columnas son MultiIndex (Open/High/... en nivel 0 y ticker en nivel 1):
    if isinstance(df.columns, pd.MultiIndex):
        # Intentamos extraer la serie Close del ticker
        try:
            close_series = df['Close'][ticker]
        except KeyError:
            # Algunas veces el ticker no está en segundo nivel; tomar la primera col de 'Close'
            close_series = df['Close'].iloc[:, 0]
        out = pd.DataFrame({
            "Date": pd.to_datetime(df[date_col]),
            "Close": pd.to_numeric(close_series, errors="coerce")
        })
    else:
        # Columnas simples: usar 'Close' o 'Adj Close' si no existe 'Close'
        close_col = "Close" if "Close" in df.columns else ("Adj Close" if "Adj Close" in df.columns else None)
        if close_col is None:
            raise ValueError("No se encontró 'Close' ni 'Adj Close' en el DataFrame descargado.")
        out = pd.DataFrame({
            "Date": pd.to_datetime(df[date_col]),
            "Close": pd.to_numeric(df[close_col], errors="coerce")
        })

    # Limpieza final
    out = out.sort_values("Date").drop_duplicates(subset=["Date"]).dropna(subset=["Close"]).reset_index(drop=True)
    if out.empty:
        raise ValueError("Tras limpiar datos, no quedan valores válidos de 'Close'.")

    return out






def etiquetar_estados(close: pd.Series, ma_window: int = 50, banda_pct: float = 0.01) -> pd.Series:
    """
    Estados: 'Alcista', 'Neutral', 'Bajista' según desviación del Close frente a SMA(ma_window).
      - Alcista: (Close - SMA)/SMA > banda_pct
      - Bajista: (Close - SMA)/SMA < -banda_pct
      - Neutral: caso contrario
    """
    sma = close.rolling(ma_window, min_periods=ma_window//2).mean()
    desv = (close - sma) / sma
    estado = pd.Series(index=close.index, dtype=object)
    estado[desv > banda_pct] = "Alcista"
    estado[desv < -banda_pct] = "Bajista"
    estado[(desv >= -banda_pct) & (desv <= banda_pct)] = "Neutral"
    # Rellenar NaN inicial como Neutral
    return estado.fillna("Neutral")


def estimar_matriz_transicion(estados: pd.Series, orden=("Alcista", "Neutral", "Bajista"), suavizado: float = 0.0) -> pd.DataFrame:
    """
    Matriz de transición (filas: estado actual, columnas: siguiente estado).
    Suavizado de Laplace para evitar ceros (p.ej., 1.0).
    """
    idx = {s: i for i, s in enumerate(orden)}
    k = len(orden)
    conteos = np.zeros((k, k), dtype=float)
    vals = estados.values
    for i in range(len(vals) - 1):
        a = idx.get(vals[i], None)
        b = idx.get(vals[i + 1], None)
        if a is not None and b is not None:
            conteos[a, b] += 1
    if suavizado > 0:
        conteos += suavizado
    filas = conteos.sum(axis=1, keepdims=True)
    filas[filas == 0] = 1.0
    P = conteos / filas
    return pd.DataFrame(P, index=orden, columns=orden)

def proyectar_distribucion(inicial: np.ndarray, P: np.ndarray, pasos: int) -> np.ndarray:
    """
    d_n = d_0 @ P^n
    """
    return inicial @ np.linalg.matrix_power(P, pasos)

def recomendar_deltas(prob: dict, config=None) -> dict:
    """
    Recomienda deltas de PUT y CALL cubierto según probabilidades.
    prob: dict {'Alcista': pA, 'Neutral': pN, 'Bajista': pB}
    config: opcional (dict) para ajustar umbrales y rangos de delta.
    Devuelve dict con {'PUT_delta': ..., 'CALL_delta': ..., 'rationale': ...}
    """
    pA = prob.get("Alcista", 0.0)
    pN = prob.get("Neutral", 0.0)
    pB = prob.get("Bajista", 0.0)

    # Config por defecto (ajústala a tu gusto)
    cfg = {
        "bullish_high": 0.50,  # prob Alcista alta
        "bearish_high": 0.40,  # prob Bajista alta
        "put_bullish_range": (0.30, 0.35),
        "put_neutral_range": (0.20, 0.25),
        "put_bearish_range": (0.10, 0.15),
        "call_bullish_range": (0.15, 0.20),
        "call_neutral_range": (0.20, 0.25),
        "call_bearish_range": (0.30, 0.35)
    }
    if config:
        cfg.update(config)

    # Lógica
    if pA >= cfg["bullish_high"]:
        put_delta = np.mean(cfg["put_bullish_range"])
        call_delta = np.mean(cfg["call_bullish_range"])
        rationale = "Prob Alcista alta: PUT más cercano (más prima), CALL cubierto más OTM."
    elif pB >= cfg["bearish_high"]:
        put_delta = np.mean(cfg["put_bearish_range"])
        call_delta = np.mean(cfg["call_bearish_range"])
        rationale = "Prob Bajista alta: PUT conservador (OTM), CALL cubierto más agresivo (cobertura)."
    elif (pN >= pA) and (pN >= pB):
        put_delta = np.mean(cfg["put_neutral_range"])
        call_delta = np.mean(cfg["call_neutral_range"])
        rationale = "Predomina Neutral: deltas intermedias (rango lateral)."
    else:
        # Caso mixto: preferimos prudencia si pB ~ pA
        if pB > pA:
            put_delta = np.mean(cfg["put_bearish_range"])
            call_delta = np.mean(cfg["call_neutral_range"])
            rationale = "Mixto con sesgo bajista: PUT conservador, CALL intermedio."
        else:
            put_delta = np.mean(cfg["put_neutral_range"])
            call_delta = np.mean(cfg["call_bullish_range"])
            rationale = "Mixto con sesgo alcista: PUT moderado, CALL más OTM."

    return {"PUT_delta": round(put_delta, 3), "CALL_delta": round(call_delta, 3), "rationale": rationale}

# ---------------------------
# Ejecución principal
# ---------------------------

if __name__ == "__main__":
    print(f"[{dt.datetime.now().isoformat()}] Descargando datos de {TICKER}...")
    datos = descargar_yahoo_robusto (TICKER, period=PERIODO, interval=INTERVALO)

    # Estados
    estados = etiquetar_estados(datos["Close"], ma_window=MA_WINDOW, banda_pct=BANDA_PCT)
    orden = ("Alcista", "Neutral", "Bajista")

    # Matriz
    M = estimar_matriz_transicion(estados, orden=orden, suavizado=SUAVIZADO_LAPLACE)
    P = M.values

    # Estado actual (último día)
    estado_actual = estados.iloc[-1]
    d0 = np.array([1.0, 0.0, 0.0]) if estado_actual == "Alcista" else (
         np.array([0.0, 1.0, 0.0]) if estado_actual == "Neutral" else np.array([0.0, 0.0, 1.0]))

    print("\nMatriz de transición estimada:\n", M)
    print("\nEstado actual:", estado_actual)
    print("\nProyecciones de probabilidades:")

    recomendaciones = []
    for n in HORIZONTES:
        dist = proyectar_distribucion(d0, P, n)
        prob = dict(zip(orden, dist))
        rec = recomendar_deltas(prob)
        recomendaciones.append((n, prob, rec))
        print(f"  D+{n}: {prob} -> PUT Δ≈{rec['PUT_delta']}, CALL Δ≈{rec['CALL_delta']} | {rec['rationale']}")

    # Guardar a CSV (opcional)
    out_probs = pd.DataFrame(
        [{"Horizon": n, **prob, **rec} for (n, prob, rec) in [(n, p, r) for (n, p, r) in recomendaciones]]
    )
    out_probs.to_csv(f"markov_deltas_{TICKER}.csv", index=False)
    print(f"\nRecomendaciones guardadas en: markov_deltas_{TICKER}.csv")
