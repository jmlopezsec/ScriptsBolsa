'''
Esta aplicación va a intentar gestionar el alta de operaciones de forma automática en IB.

'''


from ib_insync import *
import pandas as pd
from datetime import datetime
import pytz
import os

EXCEL_FILE = "ib_operaciones.xlsx"
SHEET_NAME = "RAW_IB"

IB_HOST = "127.0.0.1"
IB_PORT = 7496       # 7497 paper / 7496 real TWS
CLIENT_ID = 19


def connect_ib():
    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=CLIENT_ID, timeout=5)
    return ib


def load_existing_exec_ids():
    if not os.path.exists(EXCEL_FILE):
        return set()

    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)
        return set(df["exec_id"].astype(str))
    except Exception:
        return set()



def fetch_executions_with_commissions(ib):
    rows = []
    local_tz = pytz.timezone("Europe/Madrid")

    # Asegúrate de que hay conexión y que la sesión está activa
    # ib.connect('127.0.0.1', 7497, clientId=1)  # ejemplo TWS paper

    # fills() recoge fills recientes y suele traer commissionReport cuando está disponible
    fills = ib.fills()

    for f in fills:
        exec = f.execution
        contract = f.contract
        comm = f.commissionReport  # puede ser None si aún no llegó

        if exec is None or contract is None:
            continue
        if not contract.symbol or not exec.price or not exec.shares:
            continue

        exec_time = exec.time
        if exec_time.tzinfo is None:
            exec_time = exec_time.replace(tzinfo=pytz.UTC)
        exec_time = exec_time.astimezone(local_tz)

        multiplier = int(contract.multiplier) if contract.multiplier else 1
        quantity = abs(exec.shares)
        price = exec.price

        gross = price * quantity * multiplier
        commission = comm.commission if comm and comm.commission is not None else 0.0

        rows.append({
            "exec_id": str(exec.execId),
            "order_id": exec.orderId,
            "trade_id": None,
            "datetime": exec_time,
            "symbol": contract.symbol,
            "sec_type": contract.secType,
            "right": getattr(contract, "right", None),
            "strike": getattr(contract, "strike", None),
            "expiry": getattr(contract, "lastTradeDateOrContractMonth", None),
            "multiplier": multiplier,
            "currency": contract.currency,
            "exchange": contract.exchange,
            "side": exec.side,
            "quantity": quantity,
            "price": price,
            "gross_value": gross,
            "commission": commission,
            "net_value": gross - commission,
            "account": exec.acctNumber,
            "liquidation": exec.liquidation,
            "source": "IB_API",
            "inserted_at": datetime.now()
        })

    return pd.DataFrame(rows)


'''
def fetch_executions(ib):
    rows = []
    local_tz = pytz.timezone("Europe/Madrid")

    # IB SOLO garantiza datos completos si filtras por tiempo
    exec_filter = ExecutionFilter()
    exec_filter.time = ""   # vacío = desde inicio del día

    exec_details = ib.reqExecutions(exec_filter)

    # Forzar sincronización
    #ib.sleep(2)
    # Esperar a que lleguen los CommissionReport posteriores
    # Este wait hace que el loop de eventos procese mensajes pendientes.


    ib.waitOnUpdate(timeout=5.0)

    for ed in exec_details:
        exec = ed.execution
        contract = ed.contract
        comm = ed.commissionReport

        # Filtro duro: si no hay contrato real, no sirve
        if exec is None or contract is None:
            continue
        if not contract.symbol or not exec.price or not exec.shares:
            continue

        exec_time = exec.time
        if exec_time.tzinfo is None:
            exec_time = exec_time.replace(tzinfo=pytz.UTC)
        exec_time = exec_time.astimezone(local_tz)

        multiplier = int(contract.multiplier) if contract.multiplier else 1
        quantity = abs(exec.shares)
        price = exec.price

        gross = price * quantity * multiplier
        commission = comm.commission if comm and comm.commission else 0.0

        rows.append({
            "exec_id": str(exec.execId),
            "order_id": exec.orderId,
            "trade_id": None,
            "datetime": exec_time,
            "symbol": contract.symbol,
            "sec_type": contract.secType,
            "right": getattr(contract, "right", None),
            "strike": getattr(contract, "strike", None),
            "expiry": getattr(contract, "lastTradeDateOrContractMonth", None),
            "multiplier": multiplier,
            "currency": contract.currency,
            "exchange": contract.exchange,
            "side": exec.side,
            "quantity": quantity,
            "price": price,
            "gross_value": gross,
            "commission": commission,
            "net_value": gross - commission,
            "account": exec.acctNumber,
            "liquidation": exec.liquidation,
            "source": "IB_API",
            "inserted_at": datetime.now()
        })

    return pd.DataFrame(rows)

def update_excel(df_all):
    if df_all.empty:
        print("RAW_IB vacío, nada que escribir.")
        return

    # Quitar timezone (Excel no lo soporta)
    for col in df_all.columns:
        if pd.api.types.is_datetime64tz_dtype(df_all[col]):
            df_all[col] = df_all[col].dt.tz_localize(None)

    # Ordenar y asegurar unicidad
    df_all = df_all.drop_duplicates(subset=["exec_id"])
    df_all = df_all.sort_values("datetime")

    # ⚠️ ESCRITURA LIMPIA DESDE CERO
    with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl", mode="w") as writer:
        df_all.to_excel(writer, sheet_name=SHEET_NAME, index=False)

    print(f"RAW_IB escrita correctamente: {len(df_all)} filas.")

'''

def main():
    ib = connect_ib()
    #existing_exec_ids = load_existing_exec_ids()

    #df_all = fetch_executions(ib)
    df_all = fetch_executions_with_commissions(ib)

    update_excel(df_all)
    ib.disconnect()


if __name__ == "__main__":
    main()
