
from ib_insync import IB, Option, util

def get_oi(ib: IB, symbol: str, expiry: str, strike: float, right: str):
    opt = Option(symbol, expiry, float(strike), right, 'SMART')
    ib.qualifyContracts(opt)

    # Pedimos OI (101). Si también quieres volumen por strike, añade '100' -> '100,101'
    tkr = ib.reqMktData(opt, genericTickList='101', snapshot=False)
    ib.sleep(2)  # espera breve

    return {
        'callOI': tkr.callOpenInterest,
        'putOI' : tkr.putOpenInterest
    }

ib = IB(); ib.connect('127.0.0.1', 7496, clientId=7)
oi = get_oi(ib, 'AAPL', '20260320', 260, 'C')
print(oi)
ib.disconnect()
