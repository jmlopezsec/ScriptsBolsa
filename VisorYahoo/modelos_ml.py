from sklearn.linear_model import LinearRegression
import numpy as np

def modelo_lineal(df, columna_x='Close', columna_y='Close'):
    x = np.arange(len(df)).reshape(-1, 1)
    y = df[columna_y].values.reshape(-1, 1)
    modelo = LinearRegression().fit(x, y)
    pred = modelo.predict(x)
    return pred.flatten()
