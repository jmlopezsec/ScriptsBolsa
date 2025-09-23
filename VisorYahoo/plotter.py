'''



'''


import finplot as fplt
import pandas as pd

#El primer parámetro es el diccionario con las cotizaciones a representar
#Overlays tiene los valores / indicadores a representar.

import finplot as fplt
import pandas as pd

def graficar_activos(datos_dict, overlays=None, mismos=None):
    # Determinar qué valores comparten ventana
    tickers = list(datos_dict.keys())
    mismos = mismos or []
    principales = [t for t in tickers if t not in mismos]

    # Contar ejes necesarios
    valores = 1 if mismos else 0
    valores += len(principales)

    ejemplo = next(iter(overlays.values())) if overlays else {}
    ind_integrados = sum(1 for o in ejemplo.values() if not o['subgrafico'])
    ind_sub = sum(1 for o in ejemplo.values() if o['subgrafico'])

    total_axes = valores + ind_sub
    axs = fplt.create_plot(title='Multiactivos', rows=total_axes)

    if not isinstance(axs, list):
        axs = [axs]

    # Separar ejes
    main_axes = axs[:valores]
    sub_axes = axs[valores:]

    # Mapear tickers a ejes
    eje_map = {}
    for i, t in enumerate(principales):
        eje_map[t] = main_axes[i]
    if mismos:
        eje_comun = main_axes[-1]
        for t in mismos:
            eje_map[t] = eje_comun

    sub_index = 0
    for ticker, info in datos_dict.items():
        df = info['df'].copy()
        df.index = pd.to_datetime(df.index)

        ax = eje_map[ticker]
        fplt.candlestick_ochl(df[['Open', 'Close', 'High', 'Low']], ax=ax)

        if overlays and ticker in overlays:
            for col, props in overlays[ticker].items():
                if col not in df.columns:
                    continue
                color = props.get('color', None)
                if props['subgrafico']:
                    if sub_index < len(sub_axes):
                        fplt.plot(df[col], ax=sub_axes[sub_index], legend=f'{ticker} - {col}', color=color)
                        sub_index += 1
                else:
                    fplt.plot(df[col], ax=ax, legend=f'{ticker} - {col}', color=color)

    fplt.show()







'''


def graficar_activos(datos_dict, overlays=None, mismos=None):
    """
    Representa múltiples activos con finplot, agrupando algunos en la misma ventana si se especifica en 'mismos'.

    datos_dict: dict
        Diccionario de la forma {ticker: dataframe con OHLC y columnas extra}
    overlays: dict
        Diccionario {ticker: {columna: {'subgrafico': bool, 'color': str}}}
    mismos: list
        Lista de tickers que se deben dibujar en la misma ventana principal.
    """

    if mismos is None:
        mismos = []

    valores = len(datos_dict)
    ejemplo = next(iter(overlays.values())) if overlays else {}
    ind_integrados = sum(1 for o in ejemplo.values() if not o['subgrafico'])
    ind_sub = sum(1 for o in ejemplo.values() if o['subgrafico'])
    total_rows = 1 + (valores - len(mismos)) - 1 + ind_sub  # una fila para el grupo común + el resto en su ventana + indicadores sub

    axs = fplt.create_plot(title='Multiactivos', rows=total_rows)
    if not isinstance(axs, list):
        axs = [axs]

    main_axes = axs[:valores - len(mismos) + 1]  # primera ventana compartida + el resto
    sub_axes = axs[valores - len(mismos) + 1:]

    ventana_compartida = main_axes[0]
    ax_map = {}  # Ticker -> Axis

    idx = 1  # Para asignar nuevas ventanas después de la compartida
    for ticker in datos_dict:
        if ticker in mismos:
            ax_map[ticker] = ventana_compartida
        else:
            ax_map[ticker] = main_axes[idx]
            idx += 1

    subplot_idx = 0  # Índice para asignar subgráficos

    for ticker, df in datos_dict.items():
        df = df.copy()
        df.index = pd.to_datetime(df.index)

        fplt.candlestick_ochl(df[['Open', 'Close', 'High', 'Low']], ax=ax_map[ticker], legend=ticker)

        if overlays and ticker in overlays:
            for col, props in overlays[ticker].items():
                if col not in df.columns:
                    continue

                color = props.get('color', None)

                if props['subgrafico']:
                    if subplot_idx >= len(sub_axes):
                        print(f"Advertencia: no hay suficientes subgráficos para {ticker} - {col}")
                        continue
                    fplt.plot(df[col], ax=sub_axes[subplot_idx], legend=f'{ticker} - {col}', color=color)
                    subplot_idx += 1
                else:
                    fplt.plot(df[col], ax=ax_map[ticker], legend=f'{ticker} - {col}', color=color)

    fplt.show()
'''