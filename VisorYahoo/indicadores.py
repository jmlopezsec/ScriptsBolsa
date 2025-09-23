
'''
En este fichero / función se utilizan para hacer los calculos y/o indicadores se quieren utilizar

En los overlays si el valor subgrafico es True se pintará como un subgrafico debajo del principal
Si es False se pintará en el principal




'''

# indicators.py
'''
def aplicar_indicadores(datos_dict):
    overlays = {}

    for ticker, df in datos_dict.items():
        df['ma10'] = df['Close'].rolling(10).mean()
        df['ma20'] = df['Close'].rolling(20).mean()
        df['rsi14'] = calcular_rsi(df['Close'], 14)

        overlays[ticker] = {
            'ma10': {'subgrafico': False, 'color': '#ffaa00'},
            'ma20': {'subgrafico': True,  'color': '#00ff00'},
            'rsi14': {'subgrafico': True, 'color': '#ff00ff'}
        }

    return overlays
'''


def aplicar_indicadores(datos_dict):
    overlays = {}

    for ticker, info in datos_dict.items():
        df = info['df']

        df['ma10'] = df['Close'].rolling(10).mean()
        df['ma20'] = df['Close'].rolling(20).mean()
        df['rsi14'] = calcular_rsi(df['Close'], 14)

        overlays[ticker] = {
            'ma10': {'subgrafico': False, 'color': '#ffaa00'},
            'ma20': {'subgrafico': True,  'color': '#00ff00'},
            'rsi14': {'subgrafico': True, 'color': '#ff00ff'}
        }

    return overlays





def calcular_rsi(series, periodo=14):
    delta = series.diff()
    ganancia = delta.where(delta > 0, 0.0)
    perdida = -delta.where(delta < 0, 0.0)

    media_ganancia = ganancia.rolling(periodo).mean()
    media_perdida = perdida.rolling(periodo).mean()

    rs = media_ganancia / media_perdida
    rsi = 100 - (100 / (1 + rs))
    return rsi
