'''
Definición fichero excel del universo de opciones (Las que se van a recolectasr)

symbol: ticker.
expiry: AAAAMMDD.
right: C, P o BOTH.
mode:
    ATM_WINDOW → usa ventana alrededor del ATM (usa atm_window = nº de strikes por lado).
    EXPLICIT → usa lista exacta de strikes (strikes, separador ;).
atm_window: entero (si mode=ATM_WINDOW).
strikes: lista separada por ; (si mode=EXPLICIT).

EJEMPLO:

symbol,expiry,right,mode,atm_window,strikes
AAPL,20260320,BOTH,ATM_WINDOW,8,
INTC,20260320,P,EXPLICIT,,45;47.5;50
SPY,20260320,C,ATM_WINDOW,10,


'''


import os, asyncio, traceback
from datetime import datetime, date, timedelta
import pytz
import pandas as pd
import re


from ib_async.contract import Stock, Option
from ib_async.ib import IB
from ib_async import *


# =========================================================
#                    CONFIGURACIÓN
# =========================================================

TWS_HOST = "127.0.0.1"
TWS_PORT = 7496
CLIENT_ID = 19
MARKET_DATA_TYPE = 1

EXCEL_FILE = "open_interest.xlsx"
SHEET_NAME = "OI_RAW"

LOCAL_TZ = pytz.timezone("Europe/Madrid")

# Subyacentes y vencimientos a recolectar
SYMBOLS = {
    "AAPL": ["20260306", "20260320"],
    "SPY": ["20260320",],
    "INTC": ["20260306","20260320",]
}

# Filtro strikes: ATM +- rango
STRIKE_RANGE = 0.2  # ±20% del precio spot



EXCEL_UNIVERSE = "oi_universe.xlsx"
UNIVERSE_SHEET = "UNIVERSE"

# Estrategia de duplicados al escribir:
#   - "last_by_inserted" (por defecto): conserva la fila con mayor inserted_at
#   - "first_by_inserted": conserva la primera
#   - "max_oi": conserva la fila (date,symbol,expiry,right,strike,conId) con OI máximo
#   - "sum_oi_day": agrega OI por día (suma) en duplicados exactos
DEDUP_STRATEGY = "last_by_inserted"

# Estrategia para OI vacíos/NaN:
#   - "drop" (por defecto): descartar
#   - "zero": poner a 0
#   - "ffill": rellenar hacia delante en (date,symbol,expiry,right,strike)
MISSING_OI_STRATEGY = "drop"






# =========================================================
#                    UTILIDADES
# =========================================================

_BAD = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

#def sanitize_for_excel(df):
#    df = df.copy()
#    for c in df.select_dtypes(include=["object"]).columns:
#        df[c] = df[c].apply(lambda x: _BAD.sub("", x) if isinstance(x, str) else x)
#    return df

def load_universe_from_file(path=EXCEL_UNIVERSE, sheet=UNIVERSE_SHEET):
    """
    Lee un Excel con cabeceras al menos: symbol, expiry (cadena AAAAMMDD).
    Devuelve un diccionario del estilo:
        { "AAPL": ["20260306","20260320"], "SPY": ["20260320"], ... }
    Si no existe el fichero/hoja o no hay datos válidos, devuelve None.
    """
    try:
        if not os.path.exists(path):
            print(f"[UNIVERSE] No existe {path}. Se usará SYMBOLS por defecto.")
            return None

        dfu = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
        if dfu is None or dfu.empty:
            print(f"[UNIVERSE] Hoja {sheet} vacía en {path}. Se usará SYMBOLS por defecto.")
            return None

        # Normaliza cabeceras
        dfu.columns = [str(c).strip().lower() for c in dfu.columns]
        required = {"symbol", "expiry"}
        if not required.issubset(set(dfu.columns)):
            print(f"[UNIVERSE] Faltan columnas {required} en {sheet}. Se usará SYMBOLS por defecto.")
            return None

        # Limpieza y tipos
        dfu["symbol"] = dfu["symbol"].astype(str).str.upper().str.strip()
        dfu["expiry"] = dfu["expiry"].astype(str).str.strip()

        # Elimina filas inválidas
        dfu = dfu[(dfu["symbol"] != "") & (dfu["expiry"] != "")]
        if dfu.empty:
            print(f"[UNIVERSE] Sin filas válidas en {sheet}. Se usará SYMBOLS por defecto.")
            return None

        # Deduplicamos y ordenamos expiries
        uni = {}
        for sym, grp in dfu.groupby("symbol"):
            expiries = sorted(set(grp["expiry"].tolist()))
            if len(expiries) > 0:
                uni[sym] = expiries

        if not uni:
            print(f"[UNIVERSE] No se pudo formar el diccionario desde {path}. Se usará SYMBOLS por defecto.")
            return None

        print(f"[UNIVERSE] Cargado desde Excel: {len(uni)} símbolos.")
        return uni

    except Exception as e:
        print(f"[UNIVERSE] Error leyendo {path}: {e}. Se usará SYMBOLS por defecto.")
        return None





# Si ya tienes esta función, puedes sustituir su contenido
def sanitize_for_excel(df, to_tz=None):
    """
    Convierte columnas datetime con tz a naive (sin tz) para que Excel las acepte.
    - Si to_tz está definido (ej. LOCAL_TZ o 'Europe/Madrid'), convierte a esa zona antes de quitar tz.
    - Si no, deja en UTC pero sin tz (coherente para auditoría).
    """
    # 1) Columnas con dtype datetime tz-aware
    for col in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            if to_tz is not None:
                df[col] = df[col].dt.tz_convert(to_tz).dt.tz_localize(None)
            else:
                df[col] = df[col].dt.tz_convert('UTC').dt.tz_localize(None)
        # 2) Columnas object que podrían traer datetimes (mezcla string/datetime)
        elif pd.api.types.is_object_dtype(df[col]):
            # Intento cuidadoso: sólo en columnas que suenan a fecha/hora
            if any(k in col.lower() for k in ['time', 'date', 'dt', 'ts', 'inserted']):
                parsed = pd.to_datetime(df[col], errors='coerce', utc=True)
                if pd.api.types.is_datetime64_any_dtype(parsed):
                    if to_tz is not None:
                        df[col] = parsed.dt.tz_convert(to_tz).dt.tz_localize(None)
                    else:
                        df[col] = parsed.dt.tz_convert('UTC').dt.tz_localize(None)

    # 3) Índice datetime con tz
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        if to_tz is not None:
            df.index = df.index.tz_convert(to_tz).tz_localize(None)
        else:
            df.index = df.index.tz_convert('UTC').tz_localize(None)

    return df




def write_rows_to_excel(rows):
    try:

        if not rows:
            print("[EXCEL] Buffer vacío, no hay nada nuevo que escribir.")
            return

        df_new = pd.DataFrame(rows)

        if "open_interest" not in df_new.columns:
            print("[EXCEL] No existe la columna 'open_interest' en df_new. Filas recibidas:", df_new.columns.tolist())
            return

        df_new = df_new[df_new["open_interest"].notna()]
        df_new = df_new[df_new["open_interest"] > 0]

        if os.path.exists(EXCEL_FILE):
            df_old = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME, engine="openpyxl")
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_all = df_new

        df_all = sanitize_for_excel(df_all, to_tz=LOCAL_TZ)

        df_all = df_all.sort_values("inserted_at")
        df_all = df_all.drop_duplicates(subset=["date", "conId"], keep="first")
        df_all = df_all.sort_values(by=["date", "symbol", "expiry", "strike", "right"])



        with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl", mode="w") as w:
            df_all.to_excel(w, sheet_name=SHEET_NAME, index=False)

        print(f"[EXCEL] Actualizado {len(df_all)} filas")

    except Exception as e:
        print("[EXCEL ERROR]", e)
        traceback.print_exc()


# =========================================================
#                 RECOLECTOR DE OI
# =========================================================

def on_error(reqId, errorCode, errorString, contract):
    INFO_CODES = {200, 2103,2104, 2106, 2107, 2108, 2158}
    if errorCode in INFO_CODES:
        print ("Eliminado", errorCode, errorString)
        return
    print("[IB ERROR]", errorCode, errorString)





async def get_spot_price(ib, symbol):
    stk = Stock(symbol, "SMART", "USD")
    await ib.qualifyContractsAsync(stk)
    ib.reqMarketDataType(MARKET_DATA_TYPE)
    t = ib.reqMktData(stk, "", False, False)
    await asyncio.sleep(2)
    price = t.last or t.close
    ib.cancelMktData(stk)
    return price


async def filtra_strikes_validos(ib: IB, symbol: str, expiration: str,
                                     strikes: list[float], right: str = "C",
                                     exchange: str = "SMART") -> list[float]:
    """
        Devuelve solo los strikes que existen (listados) para (symbol, expiration, right).
        Cambia right a 'P' para puts o recorre ambos.
    """
    validos = []
    for k in strikes:
        opt = Option(symbol=symbol, lastTradeDateOrContractMonth=expiration,
                         strike=float(k), right=right, exchange=exchange, currency="USD")
        cds = await ib.reqContractDetailsAsync(opt)
        if cds:  # si hay al menos un ContractDetails, el contrato existe
            validos.append(float(k))

    return validos

async def filtra_ventana_atm(ib: IB, symbol: str, expiration: str,
                                 strikes_ventana: list[float], exchange: str = "SMART"):
    out = []
    for r in ("C", "P"):
        val = await filtra_strikes_validos(ib, symbol, expiration, strikes_ventana, right=r, exchange=exchange)
        out.append((r, val))

    return out


def pick_equity_option_chain(params, ticker, prefer_smart=True):
    """
    Selecciona la cadena de opciones 'buena' para un equity/ETF (ej. SPY, AAPL).
    - params: lista de objetos tipo OptionChain (e.g., ib_insync.objects.OptionChain)
    - ticker: símbolo base, e.g., 'SPY' o 'AAPL' (se espera en mayúsculas)
    - prefer_smart: prioriza SMART frente al resto de venues

    Retorna: el objeto OptionChain elegido o None si no hay candidato razonable.
    """

    # ---------- utilidades ----------
    def is_reasonable_chain(ch):
        """Heurística para evitar '2SPY'/'2AAPL' u otras cadenas degeneradas."""
        try:
            strikes = list(ch.strikes)
            if len(strikes) < 50:        # demasiado corta para subyacentes líquidos
                return False
            mn, mx = min(strikes), max(strikes)
            # Rango muy amplio pero plausible para la mayoría de equities/ETF
            return (mn >= 0.5) and (mx <= 10000) and (mx > mn)
        except Exception:
            return False

    def is_good_trading_class(ch):
        """Acepta exactamente la clase del ticker (evita prefijos '2', etc.)."""
        tc = getattr(ch, "tradingClass", "")
        return tc == ticker  # exact match

    # Ranking de venues como fallback si no hay SMART
    exchange_rank = ["CBOE", "AMEX", "PHLX", "BATS", "EDGX", "ISE", "MEMX",
                     "PEARL", "GEMINI", "EMERALD", "BOX", "NASDAQOM", "NASDAQBX",
                     "PSE", "SAPPHIRE", "MERCURY", "IBUSOPT", "CBOE2"]

    # ---------- 1) SMART + clase exacta + validación ----------
    if prefer_smart:
        chain = next((p for p in params
                      if getattr(p, "exchange", "").upper() == "SMART"
                      and is_good_trading_class(p)
                      and is_reasonable_chain(p)), None)
        if chain:
            return chain

    # ---------- 2) SMART + clase exacta (sin validar) ----------
    if prefer_smart:
        chain = next((p for p in params
                      if getattr(p, "exchange", "").upper() == "SMART"
                      and is_good_trading_class(p)), None)
        if chain:
            return chain

    # ---------- 3) Otros exchanges con clase exacta + validación (ranking) ----------
    for ex in exchange_rank:
        chain = next((p for p in params
                      if getattr(p, "exchange", "") == ex
                      and is_good_trading_class(p)
                      and is_reasonable_chain(p)), None)
        if chain:
            return chain

    # ---------- 4) Último recurso: cualquier exchange con clase exacta ----------
    chain = next((p for p in params if is_good_trading_class(p)), None)
    return chain






async def collect_chain(ib: IB, symbol: str, expiry: str, spot_price: float, queue: asyncio.Queue):
    print(f"[CHAIN] {symbol} {expiry}")

    stk = Stock(symbol, "SMART", "USD")
    await ib.qualifyContractsAsync(stk)

    print("Solicitamos parámetros de opciones...")

    try:
        params = await asyncio.wait_for(
            ib.reqSecDefOptParamsAsync(symbol, "", "STK", stk.conId),
            timeout=15
        )
    except asyncio.TimeoutError:
        print(f"[TIMEOUT] reqSecDefOptParams no respondió para {symbol}")
        return

    #print(params)

    #print(type(params), len(params))
    #print(sorted({getattr(p, "exchange", None) for p in params}))
    #print(sorted({(p.exchange, p.tradingClass) for p in params}))

    #chain = next((p for p in params if p.exchange == "SMART"), None)

    # Para AAPL
    chain = pick_equity_option_chain(params, ticker=symbol)
    if chain is None:
        raise RuntimeError(f"No se encontró una cadena válida para {symbol}")
    print("Elegida :", chain.exchange, chain.tradingClass, len(chain.expirations),
          len(chain.strikes))

    #if not chain:
    #    print(f"[ERROR] No se encontró cadena SMART para {symbol}")
    #    return


    strikes = sorted(chain.strikes)
    strikes = [int(x) for x in strikes]
    expirations= sorted(chain.expirations)

    if expiry not in expirations:
        print (f"No existe la expiración en {symbol}")
        return

    # strike más cercano al spot
    atm_strike = min(strikes, key=lambda k: abs(k - spot_price))
    #determino la posición dentro de lalista del strike buscado.
    idx = strikes.index(atm_strike)

    print(atm_strike)

    prev_8 = strikes[max(0, idx - 8): idx]
    next_8 = strikes[idx + 1: idx + 1 + 8]

    mis_strikes = prev_8 + [atm_strike] + next_8


    #strikes = [s for s in strikes if min_strike <= s <= max_strike]

    #print (f"mis_strikes 1 = {mis_strikes}")


    print(f"[INFO] {len(mis_strikes)} strikes filtrados")

    #Vamos a quedarnos con los únicos strikes que tienen sentido.


    mis_strikes= await filtra_ventana_atm(ib , symbol, expiry,mis_strikes,"SMART")

    #print (f"mis_strikes 2 = {mis_strikes}")

    def ensure_float(x):
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            return float(x.strip())
        raise TypeError(f"Strike inválido: {x!r}")

    tasks = []
    for right, lista_8 in mis_strikes:
        if right not in ("C", "P"):
            print("Right desconocido; lo salto:", right)
            continue
        for s in lista_8:
            try:
                #k = ensure_float(s)
                k = s
                #print(f"k= {k}")
            except Exception as e:
                print("Descarto strike:", s, e)
                continue

            opt = Option(symbol=symbol, lastTradeDateOrContractMonth=expiry,
                         strike=k, right=right, exchange="SMART", currency="USD")
            tasks.append(asyncio.create_task(fetch_option_oi(ib, opt, queue)))

    results = await asyncio.gather(*tasks, return_exceptions=True)



async def fetch_option_oi(ib: IB, opt: Option, queue: asyncio.Queue):

    #print ("Buscando opción ......")
    #print (opt)

    try:
        [optp]= await ib.qualifyContractsAsync(opt)
        ib.reqMarketDataType(MARKET_DATA_TYPE)

        ticker = ib.reqMktData(optp, '101', False, False)
        await asyncio.sleep(4.5)

        if opt.right == "C":
            openint= ticker.callOpenInterest
        else:
            openint= ticker.putOpenInterest

        row = {
            #"date": date.today().isoformat(),
            "date": (datetime.now(LOCAL_TZ) - timedelta(hours=6)).date().isoformat(),
            "symbol": opt.symbol,
            "expiry": opt.lastTradeDateOrContractMonth,
            "right": opt.right,
            "strike": opt.strike,
            "conId": opt.conId,
            "local_symbol": opt.localSymbol,
            "open_interest": openint,
            "last": ticker.last,
            "inserted_at": (datetime.now(LOCAL_TZ) - timedelta(hours=6)).date().isoformat()
        }

        #print (f"Encolamos: {row}")
        #print ("Encolamos ..")

        await queue.put(row)

        ib.cancelMktData(opt)

    except Exception as e:
        print(f"[ERROR] {opt.symbol} {opt.strike}{opt.right}", e)


# =========================================================
#                      WORKER EXCEL
# =========================================================

async def worker_excel(queue: asyncio.Queue):
    buffer = []

    print ("Entrado en excel")

    while True:
        row = await queue.get()
        #print("Sacando datos COLA")

        if row is None:   # señal de flush
            if buffer:
                write_rows_to_excel(buffer)
                buffer.clear()
            continue


        buffer.append(row)

        #print (len(buffer))

        if len(buffer) >= 10:
            #print("vamos a escribir en excel")
            write_rows_to_excel(buffer)
            buffer.clear()


# =========================================================
#                          MAIN
# =========================================================

async def main():
    print("Inicio recolección OI:", datetime.now(LOCAL_TZ))

    ib = IB()
    await ib.connectAsync(TWS_HOST, TWS_PORT, clientId=CLIENT_ID)
    print("Conectado a IB:", ib.isConnected())

    ib.errorEvent += on_error

    queue = asyncio.Queue()
    asyncio.create_task(worker_excel(queue))

    # 1) Intentar cargar el universo desde Excel (mismo formato de tu dict)
    loaded_symbols = load_universe_from_file()

    # 2) Si no hay Excel válido: usar tu diccionario por defecto
    symbols_dict = loaded_symbols if loaded_symbols else SYMBOLS


    for symbol, expiries in symbols_dict.items():
        spot = await get_spot_price(ib, symbol)
        print(f"[SPOT] {symbol} = {spot}")

        for expiry in expiries:
            await collect_chain(ib, symbol, expiry, spot, queue)

    # ---------------------------------------------------
    # 🔥 FLUSH FINAL REAL DEL WORKER EXCEL
    # ---------------------------------------------------
    await queue.put(None)  # señal de flush para el worker
    await asyncio.sleep(1)  # dejamos tiempo a que escriba el buffer

    while not queue.empty():
        await asyncio.sleep(1)

    print("Finalizado.")
    ib.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
