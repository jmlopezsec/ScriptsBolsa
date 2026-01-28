import os, sys, signal, re, traceback
import asyncio
from datetime import datetime
import pytz
import pandas as pd
import numpy as np

import ib_async
from ib_async import *

# =========================================================
#                    CONFIGURACIÓN
# =========================================================

TWS_HOST = "127.0.0.1"
TWS_PORT = 7496
CLIENT_ID = 0
MARKET_DATA_TYPE = 1

EXCEL_FILE = "ib2025.xlsx"
#EXCEL_FILE = "ibcopia.xlsx"
SHEET_NAME = "RAW_IB"

LOCAL_TZ = pytz.timezone("Europe/Madrid")

# =========================================================
#                    UTILIDADES
# =========================================================

def tz_local(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt.astimezone(LOCAL_TZ)


def remove_tz_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    for c in df2.columns:
        if pd.api.types.is_datetime64tz_dtype(df2[c]):
            df2[c] = df2[c].dt.tz_localize(None)
    return df2




def crear_fila(exec, contract):
    dt = tz_local(exec.time)
    qty = abs(exec.shares or 0)
    price = exec.price or 0.0
    tipo = exec.side
    if tipo == "BOT":
        gross = price * qty * 100 * -1
    else:
        gross = price * qty * 100

    return {
        "exec_id": str(exec.execId),
        "order_id": exec.orderId,
        "trade_id": "",
        "datetime": dt,
        "symbol": contract.symbol,
        "local_symbol": getattr(contract, "localSymbol", ""),
        "sec_type": contract.secType,
        "right": getattr(contract, "right", ""),
        "strike": getattr(contract, "strike", ""),
        "expiry": getattr(contract, "lastTradeDateOrContractMonth", ""),
        "currency": contract.currency,
        "side": exec.side,
        "shares": exec.shares,
        "price": price,
        "gross_value": gross,
        "commission": 0.0,
        "net_value": 0.0,
        "inserted_at": datetime.now(LOCAL_TZ),
        "underlying_price": None,
        "underlying_iv": None,
        "delta": None,
        "gamma": None,
        "theta": None,
        "vega": None,
        "Estado": "",
        "Bloque": "",
    }


# =========================================================
#                    EXCEL
# =========================================================

_BAD = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

def sanitize_for_excel(df):
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[c]):
            df[c] = df[c].dt.tz_localize(None)

    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].apply(lambda x: _BAD.sub("", x) if isinstance(x, str) else x)

    return df


def write_row_to_excel(row):
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









# =========================================================
#                    COLA GLOBAL
# =========================================================

cola = asyncio.Queue()


# =========================================================
#               CALLBACKS DE IB
# =========================================================

def on_ejecucion(trade, fill):
    print("[EXEC] Ejecución recibida")


def on_orden_status(trade):
    os = trade.orderStatus
    sym = trade.contract.symbol
    print(f"[ORDER] {sym} status={os.status} filled={os.filled}")


async def on_informe_comisiones(trade, fill, cr):
    if trade.contract.secType != "OPT":
        return

    fila = crear_fila(fill.execution, trade.contract)
    fila["commission"] = cr.commission

    await cola.put({"tipo": "orden", "data": fila})
    print("[COMMISSION] Orden encolada")


def on_error(reqId, errorCode, errorString, contract):
    INFO_CODES = {2103,2104, 2106, 2107, 2108, 2158}
    if errorCode in INFO_CODES:
        return
    print("[IB ERROR]", errorCode, errorString)


# =========================================================
#         ESPERA ROBUSTA DE GRIEGAS (FUTURE)
# =========================================================

def make_greeks_handler(fut: asyncio.Future):
    def _handler(t: Ticker):
        if t.lastGreeks and not fut.done():
            fut.set_result(t.lastGreeks)
    return _handler


# =========================================================
#                 WORKER PRINCIPAL
# =========================================================

async def worker_ordenes(ib: IB):
    while True:
        msg = await cola.get()
        data = msg["data"]

        try:
            symbol = data["symbol"]

            # ---------- Subyacente ----------
            ib.reqMarketDataType(MARKET_DATA_TYPE)
            stk = Stock(symbol, "SMART", "USD")
            await ib.qualifyContractsAsync(stk)

            t_stk = ib.reqMktData(stk, "", False, False)
            await asyncio.sleep(2)
            data["underlying_price"] = t_stk.last
            ib.cancelMktData(stk)

            # ---------- Opción ----------
            opt = Option(
                symbol=symbol,
                lastTradeDateOrContractMonth=data["expiry"],
                strike=data["strike"],
                right=data["right"],
                exchange="SMART",
                currency="USD",
            )

            await ib.qualifyContractsAsync(opt)

            greeks_fut = asyncio.get_event_loop().create_future()
            t_opt = ib.reqMktData(opt, "", False, False)
            t_opt.updateEvent += make_greeks_handler(greeks_fut)

            try:
                greeks = await asyncio.wait_for(greeks_fut, timeout=20)
                data["delta"] = greeks.delta
                data["gamma"] = greeks.gamma
                data["theta"] = greeks.theta
                data["vega"] = greeks.vega
                data["underlying_iv"] = greeks.impliedVol
            except asyncio.TimeoutError:
                print(f"[WARN] No llegaron griegas {symbol}")

            ib.cancelMktData(opt)

            write_row_to_excel(msg)

        except Exception as e:
            print("[WORKER ERROR]", e)
            traceback.print_exc()


# =========================================================
#                       MAIN
# =========================================================

async def main():
    print("Inicio ORDER listener:", datetime.now(LOCAL_TZ))

    ib = IB()
    await ib.connectAsync(TWS_HOST, TWS_PORT, clientId=CLIENT_ID)
    print("Conectado a IB:", ib.isConnected())

    ib.execDetailsEvent += on_ejecucion
    ib.commissionReportEvent += on_informe_comisiones
    ib.orderStatusEvent += on_orden_status
    ib.errorEvent += on_error

    asyncio.create_task(worker_ordenes(ib))

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
