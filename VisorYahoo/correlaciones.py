def correlacion_cruzada(df1, df2):
    return df1['Close'].corr(df2['Close'])
