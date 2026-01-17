import os, sys, signal
import asyncio                  #Importo características asíncronas
from datetime import datetime   #Importo datatime
from collections import defaultdict
import pytz                     #import pytz para la gestión de zonas horarias

import ib_async                  #Importo IB asincrona.
from ib_async import *
import pandas as pd
import numpy as np
import re
import traceback

#----------------- Utils -----------------------

#Para obtener la hora de Madrid
LOCAL_TZ = pytz.timezone("Europe/Madrid")

def tz_local(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt.astimezone(LOCAL_TZ)

# FIN obtener la hora de Madrid


#Función para generar la fila a almacenar, retorna un diccionario con l ainformación

def crear_fila (exec, contract):

    dt = tz_local(exec.time)
    qty = abs(exec.shares or 0)
    price = exec.price or 0.0
    gross = price * qty * 100
    #commission = comm.commission if (comm and comm.commission is not None) else None
    #net_value = gross - commission if commission is not None else None

    return {
        "exec_id": str(exec.execId),
        "order_id": exec.orderId,
        "trade_id": None,
        "datetime": dt,
        "symbol": contract.symbol,
        "local_symbol": getattr(contract, "localSymbol", None),
        "sec_type": contract.secType,
        "right": getattr(contract, "right", None),
        "strike": getattr(contract, "strike", None),
        "expiry": getattr(contract, "lastTradeDateOrContractMonth", None),
        "currency": contract.currency,
        "side": exec.side,
        "shares": exec.shares,
        "price": price,
        "gross_value": gross,
        "commission": 0.0,
        "net_value": 0.0,
        "inserted_at": datetime.now(LOCAL_TZ),
        # Métricas (si hay)
        "underlying_price": None,
        "underlying_iv": None,
        #"underlying_hv_30d": metrics.get("underlying_hv_30d") if metrics else None,
        #"option_iv": metrics.get("option_iv") if metrics else None,
        "delta": None,
        "gamma": None,
        "theta": None,
        "vega": None,
        "Estado": None,
        "Bloque": None,
    }



# -------------------- Excel en caliente --------------------

def remove_tz_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    for c in df2.columns:
        if pd.api.types.is_datetime64tz_dtype(df2[c]):
            df2[c] = df2[c].dt.tz_localize(None)
    return df2





_BAD = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

def sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1) Sin tz (Excel no soporta tz)
    for c in df.select_dtypes(include=["datetimetz"]).columns:
        df[c] = df[c].dt.tz_localize(None)

    # 2) Homogeneiza tipos problemáticos de tu dataset
    if "expiry" in df.columns:
        df["expiry"] = df["expiry"].astype(str)

    if "trade_id" in df.columns:
        df["trade_id"] = df["trade_id"].astype(str)
        df.loc[df["trade_id"].str.lower().isin(["nan", "none"]), "trade_id"] = ""

    if "underlying_iv" in df.columns:
        df["underlying_iv"] = df["underlying_iv"].astype(str)
        df.loc[df["underlying_iv"].str.lower().isin(["nan", "none"]), "underlying_iv"] = ""

    # 3) Limpia caracteres ilegales para Excel
    def _clean(x):
        return _BAD.sub("", x) if isinstance(x, str) else x

    obj_cols = df.select_dtypes(include=["object"]).columns
    if len(obj_cols):
        df[obj_cols] = df[obj_cols].applymap(_clean)

    # 4) NaN/NaT -> vacío en object, np.nan en numéricas
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].where(pd.notnull(df[c]), "")
        else:
            df[c] = df[c].where(pd.notnull(df[c]), np.nan)

    return df









def write_row_to_excel(row: dict):
    try:
        df_row = pd.DataFrame([row['data']])
        df_row = remove_tz_for_excel(df_row)

        if os.path.exists(EXCEL_FILE):
            df_existing = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME, engine="openpyxl")
            df_all = pd.concat([df_existing, df_row], ignore_index=True)
        else:
            df_all = df_row

        df_all = df_all.drop_duplicates(subset=["exec_id"], keep="last")

        if "datetime" in df_all.columns:
            df_all = df_all.sort_values(by="datetime", na_position="last")

        df_all= sanitize_for_excel (df_all)

        with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl", mode="w") as w:
            df_all.to_excel(w, sheet_name=SHEET_NAME, index=False)

        #print(f"[Excel] upsert execId={row['exec_id']} | filas={len(df_all)}")
        print ("Actualizaod fichero excel...")

    except Exception as e:
        print("[Excel ERROR]", e)
        traceback.print_exc()  # ← imprime el stack trace completo



#--------------- Definiciones --------------------------

# Configuración: usa aquí el clientId que tengas configurado como "Master API client ID" en TWS
TWS_HOST = '127.0.0.1'
TWS_PORT = 7496
CLIENT_ID = 0  # Se suscribe al cliente 0 que es el que permite interceptar los mensajes que no son suyos (general)
MARKET_DATA_TYPE  =  1


#EXCEL_FILE = "ib2025.xlsx"
EXCEL_FILE = "ibcopia.xlsx"
SHEET_NAME = "RAW_IB"



#------------------ Eventos / semaforos ----------------------------------
#Lsta de eventos funcionan como flags/semaforos Tienen los métodos set, clear y wait
#definción de eventos se poner a cero

# Evento para saber que el trade terminó (Filled / Cancelled, etc.)
Evento_orden = asyncio.Event()
#Evento para saber que se han recibido el informe de comisiones
Evento_comisiones= asyncio.Event()
#Evento de recepción Griegas
Evento_griegas= asyncio.Event()

# Evento dummy para finalizar programa
Evento_dummy= asyncio.Event()


'''
# Evento para saber que ya llegaron "al menos" una CommissionReport del trade en curso
commission_event = asyncio.Event()
# Evento para saber que ya llegaron "al menos" una CommissionReport del trade en curso
last_event = asyncio.Event()
'''




#------------------ Callbacks de Eventos de IB -------------------------
#Callback de una ejecución
#No hacemos nada
def on_ejecucion (trade: Trade, fill: Fill):

    print("Ejecución orden en TWS")
    #print (trade)


#Callback para controlar el estado de una orden. Evoluciona según evoluciona en TWS
#De momento sacamos mensajes. pero no habrá que hacer nada ya que todo se lleva al informe de comisiones

def on_orden_status(trade: Trade):
    #print("Entramos en estado de la orden")

    os = trade.orderStatus
    sym = trade.contract.localSymbol or trade.contract.symbol
    print(f"[ORDER] {sym} status={os.status} filled={os.filled} remaining={os.remaining}")


#Callback que se llama cuando se reciben las comisiones
#se forma la fila necesaria que se encola para usarlo en el main.

async def on_informe_comisiones(trade: Trade, fill: Fill, cr):

    global subyacente  # Lo declaro globalpara simpliifcar la lógica

    #print("En commision report")


    #Vamos a obtener los campos necesarios
    orden = fill.execution
    contrato = trade.contract


    if contrato.secType != 'OPT':
        return

    #print(f"[COMMISSION] execId={cr.execId} commission={cr.commission} {cr.currency}")

    datosOrden= crear_fila(orden, contrato)

    datosOrden["commission"] = cr.commission

    await cola.put({"tipo": "orden", "data": datosOrden})

    subyacente= trade.contract.symbol


    Evento_comisiones.set()
    print ("Recibidas comisiones ..")

    return


    #callback para ver actualziación de la información de las griegas


async def on_tick_griegas (t: Ticker):
    #print ("Dentro de las griegas")

    mg = t.lastGreeks
    #print (mg)
    #print (t.contract.symbol)
    sym = t.contract.symbol
    #print(mg.gamma)

    await cola.put({"tipo": "Griegas", "data": mg})

    #print(f"[{sym}] Δ={mg.delta} Γ={mg.gamma} Θ={mg.theta} V={mg.vega} IV={mg.impliedVolatility}")



    Evento_griegas.set()
    #print("Griegas Obtenidas..")




def on_error(reqId, errorCode, errorString, contract):
    #INFO_CODES = {2104, 2106, 2107, 2108, 2158}
    #if errorCode in INFO_CODES:
    #    return
    print("ERROR:", errorCode, errorString)




#--------------------------- Creación cola -----------------------------------------------
# Recoje los mensajes que se generan:
# 1.- Diccionario con la información de la orden completada.
# 2.- Cotización del subyacente
# 3.- Diccionario con los valores de la griega


cola = asyncio.Queue()


#------------------------------------------------------------------------------------------


async def main():
    #await asyncio.gather(*(descargar(i) for i in range(5)))

    print("Inicio Aplicación a las: ", datetime.now(LOCAL_TZ))



    ib= IB()        #Creo el objeto de interactive.

    print("Solicito conexión sincrona IB a las: ", datetime.now(LOCAL_TZ))
    await ib.connectAsync(TWS_HOST, TWS_PORT, clientId=CLIENT_ID)  # Solicito conexión no concurrente, sincrono
    print("Conexión sincrona IB a las: ", datetime.now(LOCAL_TZ), "Estado: ",ib.isConnected())

    #asyncio.create_task(tarea1("pepe"))
    # ReInicializo eventos para segurar su estado
    Evento_orden.clear()
    Evento_dummy.clear()
    Evento_comisiones.clear()
    Evento_griegas.clear()


    # Callbacks de suscripciones a eventos:
    ib.execDetailsEvent += on_ejecucion     #Cada vez que hay una ejecución se ejecuta la rutina on_ejecucion
    ib.commissionReportEvent +=  on_informe_comisiones    #Se ejecuta cuando llega el informe de comisiones.
    ib.orderStatusEvent += on_orden_status    #Se ejecuta cuando llega el informe de comisiones.

    ib.errorEvent += on_error                       #Se ejecuta cuando hay error

    #Espero a que se señalice el evento

    await (Evento_comisiones.wait())  # Posiblemente sólo sea necesario el del informe de comisiones.
    #await ( Evento_orden.wait() and Evento_comisiones.wait())   #Posiblemente sólo sea necesario el del informe de comisiones.


    #Cuando se llega aquí ya tenemos los datos de la orden ahora hay que solicitar cotización del subyacente y las griegas.

    # 1.- Pedimos valor del subyacente.

    # Selección del tipo de datos de mercado
    ib.reqMarketDataType(MARKET_DATA_TYPE)

    contrato = Stock(subyacente, 'SMART', 'USD')

    cds = await ib.reqContractDetailsAsync(contrato)
    # Tomamos el primero (o filtra por exchange/primaryExchange si quieres)
    cd = cds[0]
    con_id = cd.contract.conId
    # Construimos por conId (forma más precisa)
    c = Contract()
    c.conId = con_id
    c.exchange = 'SMART'
    await ib.qualifyContractsAsync(c)

    ticker_sub = ib.reqMktData(c, "", False, False)
    await asyncio.sleep(3)
    valor_subyacente = ticker_sub.last
    print (valor_subyacente)


    msg = await cola.get()
    #print(msg)
    #print (msg["data"]["expiry"])

    # --- Definimos el contrato de la opción ---
    # Ajusta a un contrato real cercano (vto, strike, right, etc.)
    opt = Option(
        symbol= msg["data"]["symbol"],
        lastTradeDateOrContractMonth= msg["data"]["expiry"],  # AAAAMMDD o AAAAMM
        strike=msg["data"]["strike"],
        right= msg["data"]["right"],
        exchange='SMART',
        currency='USD'

    )

    # Cualifica contrato para obtener conId y campos completos
    await ib.qualifyContractsAsync(opt)

    # Suscríbete a mercado: los ticks y griegas llegan por eventos
    ticker = ib.reqMktData(opt, '', False, False)  #

    print("Vamos a solicitar Griegas ......")

    ticker.updateEvent += on_tick_griegas             #callback para recibir datops en TR de las griegas

    await asyncio.sleep(12)

    await (Evento_griegas.wait())

    msggriegas = await cola.get()

    #print (msggriegas)

    if msggriegas !=  None:
        msg["data"]["delta"]= msggriegas["data"].delta
        msg["data"]["gamma"] = msggriegas["data"].gamma
        msg["data"]["theta"] = msggriegas["data"].theta
        msg["data"]["vega"] = msggriegas["data"].vega
        msg["data"]["underlying_iv"] = msggriegas["data"].impliedVol
        msg["data"]["underlying_price"]= valor_subyacente
    else:
        print(msggriegas)


    print ("Ya tenemos datos escribimos en el excel")

    write_row_to_excel(msg)

    ib.disconnect()

    print(".... Desconectado ....")

asyncio.run(main())             #Lanzar el main



