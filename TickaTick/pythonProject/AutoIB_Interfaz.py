
# -*- coding: utf-8 -*-
"""
Streamlit App: SIM ↔ IB (LIVE)
- Modo SIM: genera fills/comisiones/greeks "toy" (sin IB).
- Modo IB: se conecta a TWS/IB Gateway, escucha ejecuciones y comisiones, y en opciones pide MD para greeks/IV.
- UI: filtros, KPIs, tabla y export a CSV/Excel.
"""

import io
import time
import queue
import random
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
import pytz
import streamlit as st

# --- IB imports opcionales: sólo se usan en modo IB ---
try:
    from ib_insync import IB, Contract, ContractDetails
    IB_AVAILABLE = True
except Exception:
    IB_AVAILABLE = False

# ======================== CONFIGURACIÓN ========================
LOCAL_TZ = pytz.timezone("Europe/Madrid")

# Por defecto, conexión TWS real; ajusta en la UI si quieres.
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7496          # 7497 (paper) / 7496 (real)
DEFAULT_CLIENT_ID = 19
MARKET_DATA_TYPE = 1         # 1 = LIVE, 4 = DELAYED
GENERIC_TICKS_UNDERLYING = "106,104"  # 106=Option IV (tick 24), 104=HV 30d

# ======================== BOILERPLATE STREAMLIT ========================
st.set_page_config(page_title="SIM ↔ IB Fills", layout="wide")
st.title("Fills y Comisiones — SIM ↔ IB (LIVE)")

# ======================== ESTADO DE SESIÓN ========================
def init_state():
    if "mode" not in st.session_state:
        st.session_state.mode = "SIMULADO"  # "SIMULADO" | "IB"
    if "df" not in st.session_state:
        st.session_state.df = pd.DataFrame(columns=[
            "exec_id","order_id","datetime","symbol","local_symbol","sec_type",
            "right","strike","expiry","multiplier","currency","exchange","side",
            "shares","price","gross_value","commission","net_value","account",
            "liquidation",
            # Métricas
            "underlying_price","underlying_iv","underlying_hv_30d",
            "option_iv","delta","gamma","theta","vega","under_price_in_model",
            "md_captured_at"
        ])
    if "seen" not in st.session_state:
        st.session_state.seen = set()
    # SIM state
    if "auto_sim" not in st.session_state:
        st.session_state.auto_sim = False
    if "next_exec_id" not in st.session_state:
        st.session_state.next_exec_id = 1000000
    if "next_order_id" not in st.session_state:
        st.session_state.next_order_id = 500000
    # IB state
    if "ib" not in st.session_state:
        st.session_state.ib = None
        st.session_state.q = None
        st.session_state.close = None

init_state()

# ======================== UTILIDADES COMUNES ========================
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

def upsert_exec(row: dict):
    exec_id = row["exec_id"]
    if exec_id in st.session_state.seen:
        return
    multiplier = int(row.get("multiplier") or 1)
    qty = abs(row.get("shares") or 0)
    px  = row.get("price") or 0.0
    gross = px * qty * multiplier
    out = {
        **row,
        "gross_value": gross,
        "commission": row.get("commission"),
        "net_value": None if row.get("commission") is None else gross - row.get("commission")
    }
    st.session_state.df = pd.concat([st.session_state.df, pd.DataFrame([out])], ignore_index=True)
    st.session_state.seen.add(exec_id)

def upsert_comm(exec_id: str, commission: float, comm_ccy: str = None):
    mask = st.session_state.df["exec_id"] == exec_id
    if mask.any():
        st.session_state.df.loc[mask, "commission"] = commission
        gross = st.session_state.df.loc[mask, "gross_value"]
        st.session_state.df.loc[mask, "net_value"] = gross - commission
        if "comm_currency" not in st.session_state.df.columns:
            st.session_state.df["comm_currency"] = None
        st.session_state.df.loc[mask, "comm_currency"] = comm_ccy

def upsert_metrics(exec_id: str, metrics: dict):
    mask = st.session_state.df["exec_id"] == exec_id
    if mask.any():
        for k, v in metrics.items():
            if k in st.session_state.df.columns:
                st.session_state.df.loc[mask, k] = v

# ======================== MODO SIMULADO ========================
STOCKS = ["AAPL", "MSFT", "TSLA", "NVDA", "META", "SPY"]
EXCHS  = ["SMART", "NASDAQ", "NYSE"]
CURRS  = ["USD", "USD", "USD"]

def sim_rnd_side():
    return random.choice(["BOT", "SLD"])

def sim_rnd_price(sym):
    base = {"AAPL":200, "MSFT":400, "TSLA":250, "NVDA":800, "META":350, "SPY":500}.get(sym, 100)
    return round(np.random.normal(base, base*0.01), 2)

def sim_rnd_shares(sec_type):
    return random.choice([1,2,3,5,10]) if sec_type=="OPT" else random.choice([10,25,50,100])

def sim_fake_commission(sec_type, gross):
    if sec_type == "OPT":
        return round(0.65 + 0.5, 2)
    return round(max(1.0, 0.005 * 100), 2)

def sim_fake_option_fields(symbol):
    right = random.choice(["C","P"])
    strike = round(random.choice([*np.arange(50, 1000, 5)]), 2)
    base_date = datetime.now() + timedelta(days=random.choice([7,14,30,60]))
    expiry = base_date.strftime("%Y%m%d")
    mult = 100
    lsym = f"{symbol} {expiry} {right}{int(strike*1000)/1000:0.2f}"
    return right, strike, expiry, mult, lsym

def sim_fake_option_metrics(symbol, price_under):
    iv_under = round(max(0.05, min(1.2, np.random.normal(0.30, 0.05))), 4)
    hv_30    = round(max(0.05, min(0.80, np.random.normal(0.25, 0.06))), 4)
    iv_opt   = round(max(0.05, min(1.5, np.random.normal(iv_under + 0.02, 0.03))), 4)
    delta    = round(np.random.uniform(-0.95, 0.95), 4)
    gamma    = round(np.random.uniform(0.00, 0.15), 4)
    theta    = round(np.random.uniform(-0.50, 0.00), 4)
    vega     = round(np.random.uniform(0.00, 1.50), 4)
    return {
        "underlying_price": round(price_under, 2),
        "underlying_iv": iv_under,
        "underlying_hv_30d": hv_30,
        "option_iv": iv_opt,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "under_price_in_model": round(price_under + np.random.normal(0, 0.3), 2),
        "md_captured_at": datetime.now()
    }

def sim_make_fill():
    sec_type = random.choice(["STK","OPT","STK","OPT","STK"])
    symbol = random.choice(STOCKS)
    price_u = sim_rnd_price(symbol)
    shares = sim_rnd_shares(sec_type)
    side = sim_rnd_side()
    exch = random.choice(EXCHS)
    ccy = random.choice(CURRS)

    exec_id = str(st.session_state.next_exec_id); st.session_state.next_exec_id += 1
    order_id= st.session_state.next_order_id;    st.session_state.next_order_id += 1

    row = {
        "exec_id": exec_id,
        "order_id": order_id,
        "datetime": datetime.now(),
        "symbol": symbol,
        "local_symbol": symbol,
        "sec_type": sec_type,
        "right": None, "strike": None, "expiry": None,
        "multiplier": 1,
        "currency": ccy, "exchange": exch,
        "side": side, "shares": shares, "price": round(price_u, 2),
        "account": "DU1234567", "liquidation": 0,
        # métricas nulas por defecto
        "underlying_price": None, "underlying_iv": None, "underlying_hv_30d": None,
        "option_iv": None, "delta": None, "gamma": None, "theta": None, "vega": None,
        "under_price_in_model": None, "md_captured_at": None
    }

    metrics = {}
    if sec_type == "OPT":
        r,k,exp,mult,lsym = sim_fake_option_fields(symbol)
        row.update({"right": r, "strike": k, "expiry": exp, "multiplier": mult, "local_symbol": lsym})
        metrics = sim_fake_option_metrics(symbol, price_u)

    qty = abs(row["shares"]); mult = int(row["multiplier"]); gross = round(row["price"] * qty * mult, 2)
    comm = round(sim_fake_commission(sec_type, gross), 2)
    net  = round(gross - comm, 2)

    row["gross_value"] = gross; row["commission"] = comm; row["net_value"] = net
    row.update(metrics)
    return row

# ======================== MODO IB (LIVE) ========================
# Funciones auxiliares IB sólo si ib_insync está disponible:
if IB_AVAILABLE:
    def ib_get_underlying_contract(ib: IB, opt_contract: Contract) -> Contract:
        cds = ib.reqContractDetails(opt_contract)
        if not cds:
            raise RuntimeError("No hay ContractDetails para la opción.")
        cd: ContractDetails = cds[0]
        und = Contract(conId=cd.underConId)
        ib.qualifyContracts(und)
        return und

    def ib_req_option_and_underlying_md(ib: IB, opt: Contract):
        und = ib_get_underlying_contract(ib, opt)
        und_tk = ib.reqMktData(und, genericTickList=GENERIC_TICKS_UNDERLYING, snapshot=False, regulatorySnapshot=False)
        opt_tk = ib.reqMktData(opt, genericTickList="", snapshot=False, regulatorySnapshot=False)
        ib.waitOnUpdate(timeout=2.0)
        return opt_tk, und_tk

    def ib_extract_metrics(opt_tk, und_tk) -> dict:
        m = {
            "underlying_price": None,"underlying_iv": None,"underlying_hv_30d": None,
            "option_iv": None,"delta": None,"gamma": None,"theta": None,"vega": None,
            "under_price_in_model": None,"md_captured_at": datetime.now(LOCAL_TZ)
        }
        try:
            m["underlying_price"] = und_tk.marketPrice()
            m["underlying_iv"] = getattr(und_tk, "impliedVolatility", None)
            m["underlying_hv_30d"] = getattr(und_tk, "histVolatility", None)
            mg = getattr(opt_tk, "modelGreeks", None)
            if mg is not None:
                m["option_iv"] = mg.impliedVol
                m["delta"] = mg.delta; m["gamma"] = mg.gamma; m["theta"] = mg.theta; m["vega"] = mg.vega
                m["under_price_in_model"] = mg.undPrice
        except Exception:
            pass
        return m

    def ib_cancel_md(ib: IB, tickers):
        for t in tickers:
            try: ib.cancelMktData(t.contract)
            except Exception: pass

    @st.cache_resource
    def ib_connect_and_worker(host: str, port: int, client_id: int, mkt_type: int):
        """
        Crea la conexión IB + hilo que bombea eventos y una cola thread-safe.
        """
        ib = IB()
        ib.connect(host, port, clientId=client_id, timeout=5)
        ib.reqMarketDataType(mkt_type)

        q = queue.Queue(maxsize=50000)
        stop_flag = {"stop": False}

        def on_exec_details(ed):
            exec = ed.execution; contract = ed.contract
            q.put({
                "type": "exec",
                "exec_id": str(exec.execId),
                "order_id": exec.orderId,
                "datetime": tz_local(exec.time),
                "symbol": contract.symbol,
                "local_symbol": getattr(contract, "localSymbol", None),
                "sec_type": contract.secType,
                "right": getattr(contract, "right", None),
                "strike": getattr(contract, "strike", None),
                "expiry": getattr(contract, "lastTradeDateOrContractMonth", None),
                "multiplier": getattr(contract, "multiplier", None),
                "currency": contract.currency,
                "exchange": contract.exchange,
                "side": exec.side,
                "shares": exec.shares,
                "price": exec.price,
                "account": exec.acctNumber,
                "liquidation": exec.liquidation,
                "contract": contract  # para pedir métricas si es OPT/FOP
            })

        def on_commission(cr):
            q.put({
                "type": "comm",
                "exec_id": str(cr.execId),
                "commission": cr.commission,
                "comm_currency": cr.currency
            })

        ib.execDetailsEvent += on_exec_details
        ib.commissionReportEvent += on_commission

        def worker():
            while not stop_flag["stop"]:
                try:
                    ib.waitOnUpdate(timeout=0.2)
                except Exception:
                    time.sleep(0.2)

        import threading
        t = threading.Thread(target=worker, daemon=True); t.start()

        def close():
            stop_flag["stop"] = True
            time.sleep(0.3)
            try:
                ib.execDetailsEvent -= on_exec_details
                ib.commissionReportEvent -= on_commission
            except Exception:
                pass
            try:
                ib.disconnect()
            except Exception:
                pass

        return ib, q, close

# ======================== SIDEBAR / CONTROLES ========================
with st.sidebar:
    st.subheader("Modo de trabajo")
    st.session_state.mode = st.radio("Selecciona modo", ["SIMULADO", "IB"], index=0 if st.session_state.mode=="SIMULADO" else 1)

    if st.session_state.mode == "SIMULADO":
        st.subheader("Simulación")
        st.session_state.auto_sim = st.toggle("Auto-simulación cada 2s", value=st.session_state.auto_sim)
        n_manual = st.number_input("Fills por clic", min_value=1, max_value=10, value=1, step=1)
        btn_sim  = st.button("Simular fill(s) ahora", use_container_width=True)
    else:
        st.subheader("Conexión IB")
        host = st.text_input("Host", DEFAULT_HOST)
        port = st.number_input("Puerto", DEFAULT_PORT, step=1)
        client_id = st.number_input("Client ID", DEFAULT_CLIENT_ID, step=1)
        colb1, colb2 = st.columns(2)
        do_connect = colb1.button("Conectar", use_container_width=True, disabled=not IB_AVAILABLE)
        do_disconnect = colb2.button("Desconectar", use_container_width=True)

    st.divider()
    st.subheader("Filtros")
    sym_filter = st.text_input("Símbolo contiene…", "")
    sec_sel = st.multiselect("Tipo sec.", options=["STK","OPT","FOP"], default=["STK","OPT","FOP"])
    d_from = st.date_input("Desde", value=None)
    d_to   = st.date_input("Hasta", value=None)

    st.divider()
    st.subheader("Exportar")
    exp_csv  = st.button("Descargar CSV", use_container_width=True)
    exp_xlsx = st.button("Descargar Excel", use_container_width=True)

# ======================== LOGICA PRINCIPAL ========================
# --- SIM: manual ---
if st.session_state.mode == "SIMULADO":
    if 'btn_sim' in locals() and btn_sim:
        rows = [sim_make_fill() for _ in range(int(n_manual))]
        st.session_state.df = pd.concat([st.session_state.df, pd.DataFrame(rows)], ignore_index=True)

    # --- SIM: auto cada 2s ---
    @st.fragment(run_every="2s")
    def sim_auto_block():
        if st.session_state.auto_sim:
            how_many = random.choice([0,1,2])
            if how_many > 0:
                rows = [sim_make_fill() for _ in range(how_many)]
                st.session_state.df = pd.concat([st.session_state.df, pd.DataFrame(rows)], ignore_index=True)
    sim_auto_block()

# --- IB: conectar / desconectar ---
if st.session_state.mode == "IB":
    if not IB_AVAILABLE:
        st.error("ib_insync no está instalado o no se pudo importar. Instala: pip install ib_insync")
    else:
        if (do_connect or st.session_state.ib is None) and not do_disconnect:
            try:
                ib, q, close = ib_connect_and_worker(host, int(port), int(client_id), MARKET_DATA_TYPE)
                st.session_state.ib = ib; st.session_state.q = q; st.session_state.close = close
                st.success("Conectado a IB.")
            except Exception as e:
                st.error(f"Error al conectar: {e}")

        if do_disconnect and st.session_state.close:
            st.session_state.close()
            st.session_state.ib = None; st.session_state.q = None; st.session_state.close = None
            st.info("Desconectado.")

        # Consumir eventos IB en un fragment periódico
        @st.fragment(run_every="1s")
        def ib_consume_events():
            if st.session_state.q is None:
                return
            t0 = time.time()
            while time.time() - t0 < 0.3:  # procesar varios mensajes por ciclo
                try:
                    evt = st.session_state.q.get_nowait()
                except queue.Empty:
                    break

                if evt["type"] == "exec":
                    # Inserta ejecución (sin comisión todavía)
                    upsert_exec(evt)

                    # Si es opción, pide métricas y actualiza
                    if evt.get("sec_type") in ("OPT","FOP"):
                        try:
                            opt_tk, und_tk = ib_req_option_and_underlying_md(st.session_state.ib, evt["contract"])
                            m = ib_extract_metrics(opt_tk, und_tk)
                            ib_cancel_md(st.session_state.ib, [opt_tk, und_tk])
                            upsert_metrics(evt["exec_id"], m)
                        except Exception as e:
                            st.warning(f"Métricas opción ({evt.get('local_symbol') or evt.get('symbol')}): {e}")

                elif evt["type"] == "comm":
                    upsert_comm(evt["exec_id"], evt["commission"], evt.get("comm_currency"))

        if st.session_state.ib:
            ib_consume_events()

# ======================== FILTROS Y TABLA ========================
df_view = st.session_state.df.copy()

if sym_filter:
    mask = (df_view["symbol"].astype(str).str.contains(sym_filter, case=False, na=False)) | \
           (df_view["local_symbol"].astype(str).str.contains(sym_filter, case=False, na=False))
    df_view = df_view[mask]

if sec_sel:
    df_view = df_view[df_view["sec_type"].isin(sec_sel)]

def date_only(x):
    if pd.isna(x): return None
    if isinstance(x, datetime): return x.date()
    return x

if isinstance(d_from, date):
    df_view = df_view[df_view["datetime"].apply(date_only) >= d_from]
if isinstance(d_to, date):
    df_view = df_view[df_view["datetime"].apply(date_only) <= d_to]

if not df_view.empty:
    df_view = df_view.sort_values("datetime", na_position="last")

# ======================== KPIs + TABLA ========================
col1, col2, col3, col4 = st.columns(4)
col1.metric("Registros totales", f"{len(st.session_state.df)}")
col2.metric("Filtrados", f"{len(df_view)}")
col3.metric("Comisión total (filtro)", f"{df_view['commission'].fillna(0).sum():,.2f}")
col4.metric("Bruto total (filtro)", f"{df_view['gross_value'].fillna(0).sum():,.2f}")

st.dataframe(df_view.tail(500), use_container_width=True)

# ======================== EXPORTACIONES ========================
def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df2 = remove_tz_for_excel(df)
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df2.to_excel(writer, sheet_name="RAW_IB", index=False)
    return buf.getvalue()

if exp_csv:
    df_out = df_view.copy()
    for c in ("datetime","md_captured_at"):
        if c in df_out.columns:
            df_out[c] = df_out[c].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if isinstance(x, datetime) else "")
    st.download_button("Descargar CSV (filtro aplicado)", df_out.to_csv(index=False).encode("utf-8"),
                       file_name="fills.csv", mime="text/csv")

if exp_xlsx:
    st.download_button("Descargar Excel (filtro aplicado)", to_excel_bytes(df_view),
                       file_name="fills.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
