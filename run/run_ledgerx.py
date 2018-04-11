import asyncio, aiohttp, logging, json, argparse, datetime as dt, os
from dateutil import tz

import ledx.market, options, param, gdaxx.market


UTC_zone = tz.gettz('UTC')
NY_zone = tz.gettz('America/New_York')

def to_utc(t0):
    t0 = t0.replace(tzinfo=NY_zone)
    return t0.astimezone(UTC_zone).replace(tzinfo=None)

def accountdate(t0=None): # if past 4PM NY time account goes to next day
    if t0 is None: t0 = dt.datetime.now()
    utc = t0.replace(tzinfo=UTC_zone)
    t1 = utc.astimezone(NY_zone)
    if t1.hour>=16:
        return (t1+dt.timedelta(days=1)).date()
    else:
        return t1.date()

logging.basicConfig(format='%(asctime)s %(thread)d %(funcName)s %(levelname)s:%(message)s', filename='../log/hello_run_ledgerx.'+ accountdate().isoformat()+'.log' , level=logging.INFO)

def stopcb():
    gdaxx.market.sbreak = True
    asyncio.run_coroutine_threadsafe(ledx.market.stop(), loop)
    asyncio.run_coroutine_threadsafe(param.param.stop(), loop)

accdt = accountdate(dt.datetime.now())
options.option.accountdate = accdt

stoptime = to_utc(dt.datetime.combine(accdt, dt.time(16, 0, 10)))
logging.info('stop scheduled for ' + str(stoptime))


spot = None
async def Fbook(b,a):
    print(b,a)
    global spot
    if b is None:
        # cancel quotes
        if spot is None: return
        logging.warning('Pulling quotes')
        spot = None
        return await ledx.market.twosided.cancel()
    spot = (b+a)/2.
    if (ledx.market.twosided.market is None) or (not ledx.market.twosided.market.live):
        print('not ready')
        return
    try:
        if 'DAS' in ledx.market.contract.labels:
            das = ledx.market.contract.labels['DAS'].twosided
            if not args.nodas:
                await das.on_spot(b,a)
            if args.options:
                await options.option.on_spot(b-das.bspread,a+das.aspread)
    except Exception as e:
        logging.error('ex2 Fbook ' + str(e))


async def main(db=None, Fc=None):
    async with aiohttp.ClientSession() as session:
        coros = [gdaxx.market.get_last_price(session, Fbook)]
        coros.append(ledx.market.main1(session, db=db, Fc=Fc))
        await asyncio.wait(coros)


parser = argparse.ArgumentParser()
parser.add_argument('-options', action='store_true')
parser.add_argument('-db', action='store_true')
parser.add_argument('-nodas', action='store_true')

args = parser.parse_args()

loop = asyncio.get_event_loop()

ledx.market.order.loop = loop

loop.call_at(loop.time()+(stoptime-dt.datetime.now()).total_seconds(), stopcb)

loop.run_until_complete(main(db=args.db, Fc=options.createOptions if args.options else None))

