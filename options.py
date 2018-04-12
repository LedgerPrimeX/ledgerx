import asyncio, aiohttp, logging, json, datetime as dt, dateutil.parser, numpy as np, math
import ledx.market, param, collections

'''
http://vollib.org/documentation/python/1.0.2/
'''

import py_vollib.black_scholes_merton as bs
import py_vollib.black_scholes_merton.implied_volatility as bsi
import py_vollib.black_scholes_merton.greeks.analytical as bsg
from py_vollib.ref_python.black import d1 as vld1
from py_vollib.ref_python.black import d2 as vld2


class option(object):
    accountdate = None # set from run/run_ledgerx.py, used to alias from 'Day-Call-$10000' etc.
    spot = None
    options = {}
    labels = {}
    irate = 0.016
    time_exp = {} # if exp in this dict use the given time2exp
    exps = {}
    inflight = False
    last = dt.datetime.now()
    pvol = None # the param 'option.vol', set in createOptions() below
    sqr2pi = np.sqrt(2.*np.pi)
    nvol = {'call' : 1., 'put' : 1.}
    das = None # pointing to swap to retrieve olean
    @staticmethod
    def factory(msg):
        if msg['id'] not in option.options:
            option(msg)
        return option.options[msg['id']]
    @staticmethod
    def clear_time():
        option.time_exp.clear()
    @staticmethod
    def set_time(t0):
        option.time_exp = {i:j.timetoexp(t0) for i,j in option.exps.items()}
    def __init__(self, msg, ctr=None):
        self.ctr = ctr
        self.msg = msg
        self.rate = option.irate
        self.exp  = dateutil.parser.parse(self.msg['date_expires']).replace(tzinfo=None)
        self.date = self.exp.date().isoformat()
        self.live = dateutil.parser.parse(self.msg['date_live']).replace(tzinfo=None)
        option.exps[self.exp] = self # overwrite ok, just need the exp and an option to calc time2exp
        K =  self.msg['strike_price']/100.
        self.K = K #bs.strike(K, K, self.timetoexp(), 1)
        self.id = self.msg['id']
        option.options[self.id] = self
        option.labels[self.msg['label']] = self
        logging.info(self.msg['label'])
        self.day = 'BTC-Day' if self.exp.date() == option.accountdate else None
        self.dayK = ('BTC-Day-' + ('Put' if self.type == 'put' else 'Call') + '-' + '${:0,.0f}'.format(int(self.K))) if self.exp.date() == option.accountdate else None
        if self.day is not None:
            logging.info('Day:' + self.day + ' ' + self.dayK)
        self.flag = 'p' if self.type == 'put' else 'c'
        self.roundf = 10
        self.blean = 0
        self.alean = 0
        self.vmult = 10 # quote = tv + vmult*vega + alean
        self.vvol = 0.7 if self.flag == 'c' else 0.8
        self.dvol = 0 # exp level
        self.dcpvol = 0 # put/call exp level
        self.mlean = 1 # multiplicative to a/blean at expiration level
        self.status = False
    @property
    def vol(self):
        return self.vvol + self.dvol + self.dcpvol
    def __str__(self):
        return self.msg['label']
    @property
    def type(self):
        return self.msg['type']
    def is_call(self):
        return self.flag == 'c'
    def timetoexp(self, t0 = None):
        x = self.exp - (dt.datetime.now() if t0 is None else t0)
        return (x.days * 24 + x.seconds/3600) / (365.25 * 24)
    def theo(self, S, vol, t0=None):
        if t0 is None:
            if self.exp not in option.time_exp:
                option.time_exp[self.exp] = self.timetoexp()
            te = option.time_exp[self.exp]
        else:
            te = self.timetoexp(t0)
            if te <= 0: te = 0
            option.time_exp[self.exp] = te
        if te<0:
            self.tv = np.nan
        else:
            self.tv = bs.black_scholes_merton(self.flag, S, self.K, te, self.rate, vol, 0.)
        return self.tv
    def delta(self, S, vol, t0 = None):
        if t0 is None:
            if self.exp not in option.time_exp:
                option.time_exp[self.exp] = self.timetoexp()
            te = option.time_exp[self.exp]
        else:
            te = self.timetoexp(t0)
            if te <= 0: te = 0
            option.time_exp[self.exp] = te
        return bsg.delta(self.flag, S, self.K, te, self.rate, vol, 0.)
    def greeks(self, S, vol, t0 = None, pr=None):
        tv = self.theo(S, vol, t0)
        d1 = self.d1(S, vol)
        te = option.time_exp[self.exp]
        try:
            ivol = self.ivol(S, pr) if pr is not None else None
        except:
            ivol = None
        return {'tv': tv, 'delta':self.delta(S, vol), 'gamma':self.gamma(S, vol), 'vega':self.vega(S, vol), 'theta':self.theta(S, vol),
                 'd1':d1, 'd2':self.d2(S, vol), 'te':te, 'S':S, 'vol':vol, 'v/v0':np.exp(-d1*d1/2.), 'v0': S*np.sqrt(te)/option.sqr2pi, 'ivol' : ivol, 'rho': self.rho(S,vol)}
    def gamma(self, S, vol):
        if option.time_exp[self.exp] <= 0: return 0.
        return bsg.gamma(self.flag, S, self.K, option.time_exp[self.exp], self.rate, vol, 0.)
    def vega(self, S, vol):
        return bsg.vega(self.flag, S, self.K, option.time_exp[self.exp], self.rate, vol, 0.)
    def rho(self, S, vol):
        return bsg.rho(self.flag, S, self.K, option.time_exp[self.exp], self.rate, vol, 0.)
    def theta(self, S, vol):
        if option.time_exp[self.exp] <= 0: return 0.
        return bsg.theta(self.flag, S, self.K, option.time_exp[self.exp], self.rate, vol, 0.)
    def ivol(self, S, pr):
        return bsi.implied_volatility(pr, S, self.K, option.time_exp[self.exp], self.rate, 0., self.flag)
    def itm(self, S):
        return self.K<S if self.flag == 'c' else self.K>S
    def d1(self, S, sigma):
        te = option.time_exp[self.exp]
        F = S*np.exp((self.rate)*te)
        return vld1(F, self.K, te , self.rate, sigma) # keep r argument for consistency
    def d2(self, S, sigma):
        te = option.time_exp[self.exp]
        F = S*np.exp((self.rate)*te)
        return vld2(F, self.K, te , self.rate, sigma) # keep r argument for consistency
    def roundq(self, x):
        f = self.roundf *10 ** (math.floor(math.log10(x)) - 2)
        return math.floor(x/f)*f, math.ceil(x/f)*f
    def set_x(self, x, entry):
        if entry not in x: return
        logging.info(str(self) + ' set '+ entry + ' ' + str(x[entry]))
        if 'vol' in x[entry]:
            self.vvol = float(x[entry]['vol'])
        if 'dvol' in x[entry]:
            self.dvol = float(x[entry]['dvol'])
        if 'dcvol' in x[entry] and self.is_call():
            self.dcpvol = float(x[entry]['dcvol'])
        if 'dpvol' in x[entry] and not self.is_call():
            self.dcpvol = float(x[entry]['dpvol'])
        if 'round' in x[entry]:
            self.roundf = max(float(x[entry]['round']), 0.1)
        if 'blean' in x[entry]:
            self.blean = float(x[entry]['blean'])
        if 'alean' in x[entry]:
            self.alean = float(x[entry]['alean'])
        if 'mlean' in x[entry]:
            self.mlean = max(1, float(x[entry]['mlean']))
        if 'rate' in x[entry]:
            self.rate = float(x[entry]['rate'])
        if 'vmult' in x[entry]:
            self.vmult = float(x[entry]['vmult'])
        if self.ctr is None:
                return
        if 'bsize' in x[entry]:
            self.ctr.twosided.bsize = int(x[entry]['bsize'])
        if 'asize' in x[entry]:
            self.ctr.twosided.asize = int(x[entry]['asize'])
        if 'status' in x[entry]:
            self.status = x[entry]['status']
            if not self.status:
                asyncio.ensure_future(self.ctr.twosided.canceltwo())

    @staticmethod
    async def on_vol(x):
        logging.info('option vols')
        option.on_pvol(x)

    @staticmethod
    def on_pvol(x):
        for i,j in option.options.items():
            if j.ctr is not None and j.ctr.twosided.option is None:
                j.ctr.twosided.option = j
            j.set_x(x, j.type) # 'call' or 'put
            if j.day is not None:
                j.set_x(x, j.day) # only not None for daily options, or on day of expiration of regular options
            j.set_x(x, j.date) # '2017-11-24'
            if j.dayK is not None:
                j.set_x(x, j.dayK)
            j.set_x(x, str(j.id)) # '12345454' (not 'BTC 2017-11-24 Call $7000.00', since mongo/bson does not like keys with '.'s in it I think)

    def on_fill(self, msg):
        option.clear_time()
        if 'BTCUSD' in msg and msg['BTCUSD'] is not None:
            pr = msg['filled_price']/100.
            gr = self.greeks(msg['BTCUSD'], self.vol, pr=pr)
            ask = msg['is_ask']
            sz = msg['filled_size']
            dlean = pr/10. # 5%
            tef = min(1/np.sqrt(gr['te']), 10)
            logging.info('option trade ' + str(msg) + ' ' + str(gr) + ' dlean: ' + str(dlean*tef))
            if ask:
                self.alean += dlean*tef
                self.ctr.twosided.asize = 1
            else:
                self.blean += dlean*tef
                self.ctr.twosided.bsize = 1


    @staticmethod
    async def on_spot(b,a):
        if option.inflight or (ledx.market.twosided.market is None) or (not ledx.market.twosided.market.live):
            return
        if option.das is None:
            option.das = ledx.market.contract.labels['DAS'].twosided
        t = dt.datetime.now()
        if (t-option.last).seconds < 5: return
        option.last = t
        option.inflight = True
        S = (b+a)/2+option.das.olean
        option.clear_time()
        for i,j in option.options.items():
            vol = j.vol
            tv = j.theo(S,vol)
            vega = j.vega(S,vol)
            b = tv - j.vmult*vega - j.mlean*j.blean
            a = tv + j.vmult*vega + j.mlean*j.alean
            b = j.roundq(b)[0] if b > 0 else -1
            a = j.roundq(a)[1]-0.01 if a > 12.5 else -1
            if j.status and j.ctr.twosided.ok:
                j.ctr.twosided.ok = False
                await j.ctr.twosided.on_quote(b, a)
        option.inflight = False


def createOptions():
    option.pvol = param.param('option.vol', option.on_vol)
    for i,j in ledx.market.contract.contracts.items():
        try:
            if 'option' in j.msg['derivative_type'] and j.msg['active']:
                x = option(j.msg, j)
        except Exception as e:
            logging.warning(e)
    logging.info('options created ' + str(len(option.options)))


