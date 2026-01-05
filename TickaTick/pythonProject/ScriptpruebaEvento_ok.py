# test_exec_events_fixed.py
from ib_insync import IB, ExecutionFilter

def on_exec_details(trade, fill):
    exec = fill.execution
    contract = trade.contract

    print(
        f"[execDetails] execId={exec.execId} "
        f"sym={contract.localSymbol or contract.symbol} "
        f"px={exec.price} qty={exec.shares} "
        f"side={exec.side} exch={exec.exchange}"
    )

def on_commission_report(trade, fill, report):
    print(
        f"[commissionReport] execId={report.execId} "
        f"commission={report.commission} {report.currency}"
    )

INFO_CODES = {2104, 2106, 2107, 2108}

def on_error(reqId, errorCode, errorString, contract):
    if errorCode in INFO_CODES:
        return
    print(f"[ERROR] {reqId} {errorCode} {errorString}")


def main():
    ib = IB()

    # clientId=0 + reqAutoOpenOrders(True) → órdenes manuales TWS
    ib.connect("127.0.0.1", 7496, clientId=0, timeout=5)
    ib.reqMarketDataType(1)
    ib.reqAutoOpenOrders(True)

    # Pull inicial
    eds = ib.reqExecutions(ExecutionFilter())
    print(f"[PULL] reqExecutions devolvió {len(eds)} filas")

    # Eventos correctos
    ib.execDetailsEvent += on_exec_details
    ib.commissionReportEvent += on_commission_report
    ib.errorEvent += on_error

    print("Esperando ejecuciones y comisiones...")
    ib.run()

if __name__ == "__main__":
    main()
