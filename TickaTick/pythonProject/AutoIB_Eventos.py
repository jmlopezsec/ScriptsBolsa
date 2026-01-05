
# -*- coding: utf-8 -*-
"""
ib_execs_with_greeks.py (versión corregida)
- clientId=0 + reqAutoOpenOrders(True) para bind de órdenes MANUALES TWS.
- Eventos con la firma que usas: execDetails(trade, fill), commissionReport(trade, fill, report).
- Métricas de opciones con ib.sleep() (cooperativo) en vez de waitOnUpdate() dentro del handler.
- Escritura en Excel en caliente; dedupe por exec_id conservando la ÚLTIMA fila (la que incluye comisión).
"""

import os, sys, signal
from datetime import datetime
from collections import defaultdict

import pandas as pd
import pytz
from ib_insync import IB, ExecutionFilter, Contract, ContractDetails

# -------------------- Config --------------------
EXCEL_FILE = "ib_operaciones3.xlsx"
SHEET_NAME = "RAW_IB"

IB_HOST = "127.0.0.1"
IB_PORT = 7496           # 7497 papel / 7496 real
CLIENT_ID = 0            # para bind de órdenes manuales
MARKET_DATA_TYPE = 1     # 1 LIVE / 4 DELAYED

GENERIC_TICKS_UNDERLYING = "106,104"  # 106=OPTION_IV (tick 24), 104=HV 30d
LOCAL_TZ = pytz.timezone("Europe/Madrid")

# -------------------- Utiles --------------------
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

# -------------------- Excel en caliente --------------------
def write_row_to_excel(row: dict):
    """
    Escribe/actualiza 1 fila en Excel:
    - fusiona con existente
    - dedupe por exec_id conservando la ÚLTIMA aparición (keep='last')
    - ordena por datetime
    """
    df_row = pd.DataFrame([row])
    df_row = remove_tz_for_excel(df_row)

    if os.path.exists(EXCEL_FILE):
        try:
            df_existing = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME, engine="openpyxl")
        except Exception:
            df_existing = pd.DataFrame(columns=df_row.columns)
        df_all = pd.concat([df_existing, df_row], ignore_index=True)
    else:
        df_all = df_row

    # dedupe conservando la ÚLTIMA (la más reciente, normalmente con comisión):
    df_all = df_all.drop_duplicates(subset=["exec_id"], keep="last")

    if "datetime" in df_all.columns:
        df_all = df_all.sort_values(by="datetime", na_position="last")

    with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl", mode="w") as w:
        df_all.to_excel(w, sheet_name=SHEET_NAME, index=False)

    print(f"[Excel] upsert execId={row['exec_id']} | filas totales={len(df_all)}")

# -------------------- Métricas opciones --------------------
def get_underlying_contract(ib: IB, opt_contract: Contract) -> Contract:
    cds = ib.reqContractDetails(opt_contract)
    if not cds:
        raise RuntimeError("No hay ContractDetails para la opción.")
    cd: ContractDetails = cds[0]
    und = Contract(conId=cd.underConId)
    ib.qualifyContracts(und)
    return und

def subscribe_md_for_option_and_underlying(ib: IB, opt: Contract):
    und = get_underlying_contract(ib, opt)
    und_tk = ib.reqMktData(und, genericTickList=GENERIC_TICKS_UNDERLYING, snapshot=False, regulatorySnapshot=False)
    opt_tk = ib.reqMktData(opt, genericTickList="", snapshot=False, regulatorySnapshot=False)
    return opt_tk, und_tk

def extract_metrics(opt_tk, und_tk):
    m = {
        "underlying_price": und_tk.marketPrice(),
        "underlying_iv": getattr(und_tk, "impliedVolatility", None),  # tick 24 OPTION_IV
        "underlying_hv_30d": getattr(und_tk, "histVolatility", None),
        "option_iv": None, "delta": None, "gamma": None, "theta": None, "vega": None,
        "under_price_in_model": None,
        "md_captured_at": datetime.now(LOCAL_TZ)
    }
    mg = getattr(opt_tk, "modelGreeks", None)
    if mg is not None:
        m["option_iv"] = mg.impliedVol
        m["delta"] = mg.delta
        m["gamma"] = mg.gamma
        m["theta"] = mg.theta
        m["vega"] = mg.vega
        m["under_price_in_model"] = mg.undPrice
    return m

def cancel_md(ib: IB, tickers):
    for t in tickers:
        try:
            ib.cancelMktData(t.contract)
        except Exception:
            pass

# -------------------- Main --------------------
def main():
    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=CLIENT_ID, timeout=5)
    ib.reqMarketDataType(MARKET_DATA_TYPE)
    ib.reqAutoOpenOrders(True)  # bind de órdenes manuales nuevas (clientId=0 obligatorio)

    # Almacén por execId
    store = defaultdict(lambda: {"exec": None, "contract": None, "comm": None, "metrics": None})

    # Pull inicial del día (VERIFICACIÓN)
    try:
        eds = ib.reqExecutions(ExecutionFilter())
        print(f"[PULL] reqExecutions devolvió {len(eds)} filas")
        for ed in eds:
            execId = str(ed.execution.execId)
            store[execId]["exec"] = ed.execution
            store[execId]["contract"] = ed.contract
    except Exception as e:
        print("[PULL] Error:", e)

    def build_row(exec, contract, comm, metrics):
        dt = tz_local(exec.time)
        multiplier = int(getattr(contract, "multiplier", 1) or 1)
        qty = abs(exec.shares or 0)
        price = exec.price or 0.0
        gross = price * qty * multiplier
        commission = comm.commission if (comm and comm.commission is not None) else None
        net_value = gross - commission if commission is not None else None

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
            "multiplier": multiplier,
            "currency": contract.currency,
            "exchange": contract.exchange,
            "side": exec.side,
            "shares": exec.shares,
            "price": price,
            "gross_value": gross,
            "commission": commission,
            "net_value": net_value,
            "account": exec.acctNumber,
            "liquidation": exec.liquidation,
            "source": "IB_API",
            "inserted_at": datetime.now(LOCAL_TZ),
            # Métricas (si hay)
            "underlying_price": metrics.get("underlying_price") if metrics else None,
            "underlying_iv": metrics.get("underlying_iv") if metrics else None,
            "underlying_hv_30d": metrics.get("underlying_hv_30d") if metrics else None,
            "option_iv": metrics.get("option_iv") if metrics else None,
            "delta": metrics.get("delta") if metrics else None,
            "gamma": metrics.get("gamma") if metrics else None,
            "theta": metrics.get("theta") if metrics else None,
            "vega": metrics.get("vega") if metrics else None,
            "under_price_in_model": metrics.get("under_price_in_model") if metrics else None,
            "md_captured_at": metrics.get("md_captured_at") if metrics else None,
        }

    # -------------------- Handlers (firmas = tu test) --------------------
    def on_exec_details(trade, fill):
        exec = fill.execution
        contract = trade.contract
        execId = str(exec.execId)

        store[execId]["exec"] = exec
        store[execId]["contract"] = contract

        print(f"[execDetails] execId={execId} sym={contract.localSymbol or contract.symbol} "
              f"px={exec.price} qty={exec.shares} side={exec.side} exch={exec.exchange}")

        # Métricas para opciones sin bloquear el loop
        metrics = None
        if contract.secType in ("OPT", "FOP"):
            try:
                opt_tk, und_tk = subscribe_md_for_option_and_underlying(ib, contract)
                # Espera cooperativa ~1s para que lleguen ticks (evita "event loop is running")
                ib.sleep(1.0)
                metrics = extract_metrics(opt_tk, und_tk)
                cancel_md(ib, [opt_tk, und_tk])
            except Exception as e:
                print(f"[MD] Error métricas para {contract.localSymbol or contract.symbol}: {e}")

        # Emitir fila en caliente (commission puede ser None por ahora)
        row = build_row(exec, contract, store[execId]["comm"], metrics)
        write_row_to_excel(row)

    def on_commission_report(trade, fill, report):
        execId = str(report.execId)
        store[execId]["comm"] = report
        print(f"[commissionReport] execId={execId} commission={report.commission} {report.currency}")

        # Si ya teníamos exec/contract, actualizar fila en Excel con comisión
        d = store[execId]
        if d["exec"] is not None and d["contract"] is not None:
            row = build_row(d["exec"], d["contract"], d["comm"], d["metrics"])
            write_row_to_excel(row)

    INFO_CODES = {2104, 2106, 2107, 2108}
    def on_error(reqId, errorCode, errorString, contract):
        if errorCode in INFO_CODES:
            return
        print(f"[ERROR] {reqId} {errorCode} {errorString}")

    ib.execDetailsEvent += on_exec_details
    ib.commissionReportEvent += on_commission_report
    ib.errorEvent += on_error

    print(f"Conectado a {IB_HOST}:{IB_PORT} clientId={CLIENT_ID} | MarketDataType={MARKET_DATA_TYPE}")
    print("Escuchando ejecuciones y comisiones... (Ctrl+C para salir)")

    def flush_and_exit(signum=None, frame=None):
        try:
            print("Desuscribiendo y desconectando...")
            ib.execDetailsEvent -= on_exec_details
            ib.commissionReportEvent -= on_commission_report
            ib.errorEvent -= on_error
            ib.disconnect()
        finally:
            sys.exit(0)

    signal.signal(signal.SIGINT, flush_and_exit)
    signal.signal(signal.SIGTERM, flush_and_exit)

    ib.run()

if __name__ == "__main__":
    main()
