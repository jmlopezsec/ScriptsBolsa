#Definción de las constantes del entorno

DEFAULT_PERIOD = '2mo'
DEFAULT_INTERVAL = '1d'

#valores válidos periodo
'''
"1d"	1 día
"5d"	5 días
"1mo"	1 mes
"3mo"	3 meses
"6mo"	6 meses
"1y"	1 año
"2y"	2 años
"5y"	5 años
"10y"	10 años
"ytd"	Desde el inicio del año
"max"	Todos los datos disponibles
'''
#Valores válidos intervalo
'''
"1m"	1 minuto (máx. 7 días)
"2m"	2 minutos (máx. 60 días)
"5m"	5 minutos (máx. 60 días)
"15m"	15 minutos (máx. 60 días)
"30m"	30 minutos (máx. 60 días)
"60m"	1 hora (máx. 730 días)
"90m"	90 minutos (algunos activos)
"1d"	Diario
"5d"	Cada 5 días
"1wk"	Semanal
"1mo"	Mensual
"3mo"	Trimestral
'''

'''
⚠️ Restricciones: Algunos interval sólo están disponibles para ciertos period. 
Por ejemplo, no puedes pedir 1 año de datos con intervalo de 1 minuto.
'''
#prueba