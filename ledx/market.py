import asyncio, aiohttp, async_timeout, logging, datetime as dt, collections, ujson, pymongo, time
import key, param

baseurl = key.keys['ledgerx-baseurl'] #''test.ledgerx.com/api'
LOG = logging.getLogger('')

class login(object):
    def __init__(self, db=False):
        self.token = None
        self.live = False
        self.types = collections.defaultdict(list)
        self.mpid = key.keys['ledgerx-mpid']
        self.cid = key.keys['ledgerx-cid']
        self.token = key.keys['ledgerx-token']
        if db:
            self.motor = pymongo.MongoClient()
            self.db = self.motor[key.keys['ledgerx-market']]
        else:
            self.db = None
    def __del__(self):
        self.live = False
    async def auth(self, session):
        self.session = session
        self.live = True

    async def me(self):
        url = 'https://' + baseurl + '/users/mp/clients'
        async with self.session.get(url, params={'token': self.token}) as req:
            if req.status == 521:
                print('server down?', req.get())
                exit()
            resp = await req.json()
            if 'data' not in resp:
                logging.error('no data me', resp)
                print('me', resp)
                return
            self.me = {x['cid']:x for x in resp['data']}
            print('me:', self.me, 'mpid:', self.mpid, 'cid:', self.cid)

    async def contracts(self):
        url = 'https://' + baseurl + '/contracts'
        async with self.session.get(url, data = {'token': self.token, 'limit':0}) as req: #'after_ts': '2017-10-12T04:00:00.000Z', 'after_ts': dt.datetime(2017,1,1).isoformat()}) as req: #(dt.datetime.now()-dt.timedelta(days=1)).isoformat()}) as req:
            resp = await req.json()
            print('contracts len:', len(resp['data']))
            for x in resp['data']:
                c = contract(x)
        self.send_db(['contracts'], {'contracts': resp['data']})

    def send_db(self, w, x):
        if 'time' not in x: x['time'] = time.time()
        if self.db is not None:
            for i in w:
                self.db[i].insert_one(x)

    async def orders(self):
        url = 'https://' + baseurl + '/orders'
        async with self.session.get(url, params={'token': self.token, 'limit': 0}) as req:
            resp = await req.json()
            print('orders', resp)
            if 'data' not in resp:
                logging.error('no data orders', resp)
                return
            print('orders', len(resp['data']), resp['meta'])
            for x in resp['data']:
                print('orders', x)


class contract(object):
    contracts = {}
    labels = {}
    das = None

    @staticmethod
    def factory(msg):
        id = msg['id']
        if id in contract.contracts:
            return contract.contracts[id]
        else:
            return contract(msg)

    def __init__(self, msg):
        self.id = msg['id']
        self.msg = msg
        self.pos = None
        self.top = None
        contract.contracts[self.id] = self
        if 'swap' in msg['derivative_type'] and msg['active']:
            print('contract:', self.id, msg)
            contract.labels['DAS'] = self
            self.twosided = dayswap(self)
        elif msg['active']:
            self.twosided = twosided(self)
        contract.labels[msg['label']] = self

    def __str__(self):
        return self.msg['label']

    def set_position(self, sz):
        self.pos = sz
        if sz != 0: print('pos', self.msg, sz)

    def book_top(self, x):
        self.top = x
        if x['ask'] > 0:
            pass

    def mpid_action(self, data):
        x = order.factory(self, data)
        x.action(data)

    async def book_states(self, auth):
        url = 'https://' + baseurl + '/book-states/' + str(self.id)
        async with auth.session.get(url, params={'token': auth.token}) as req:
            resp = await req.json()
            self.book = resp

    @staticmethod
    def positions(data):
        for x in data['positions']:
            id = x['contract']['id']
            if id not in contract.contracts:
                logging.warning('contract positions no id ' + str(id) + ' ' + str(x))
                continue
            contract.contracts[id].set_position(x['size'])

sbreak = False

async def stop():
    global sbreak
    logging.info('ledgerx stop called')
    sbreak = True


class market(object):
    auth = None
    hb = dt.datetime.now()
    def __init__(self):
        self.live = False
        self.contacts = {}

    async def start(self):
        await self.events()

    async def book_top(self):
        url = 'https://' + baseurl + '/book-tops'
        async with market.auth.session.get(url, data={'token': market.auth.token}) as req:
            resp = await req.json()
            if req.status != 200: return
            print('book-top', len(resp['data']), resp['data'][0])
            for x in resp['data']:
                contract.contracts[x['contract_id']].book_top(x)

    async def events(self):
        while not sbreak:
            logging.info('ledgerx starting market ws')
            try:
                async with market.auth.session.ws_connect('wss://' + baseurl + '/ws?token=' + self.auth.token) as ws:
                    self.live = True
                    logging.info('ledgerx started market ws')
                    while not sbreak:
                        with async_timeout.timeout(60, loop=order.loop):
                            msg = await ws.receive()
                            try:
                                data = ujson.loads(msg.data)
                            except TypeError as e:
                                logging.warning('ledgerx ws error ' + str(msg.data) + str(e))
                                raise StopIteration
                            if data['type'] == 'heartbeat':
                                market.hb = dt.datetime.now()
                                if sbreak:
                                    logging.info('ledgerx receive stop')
                                    break
                                continue
                            if data['type'] == 'open_positions_update':
                                contract.positions(data)
                            elif data['type'] == 'action_report':
                                if 'mpid' in data and data['mpid'] == self.auth.mpid:
                                    if 'contract_id' in data and data['contract_id'] in contract.contracts:
                                        contract.contracts[data['contract_id']].mpid_action(data)
                                    market.auth.send_db(['orders'], data)
                                else:
                                    market.auth.send_db(['market1'], data)
                            elif data['type'] == 'book_top':
                                if 'contract_id' in data and data['contract_id'] in contract.contracts:
                                    contract.contracts[data['contract_id']].book_top(data)
                            elif data['type'] == 'collateral_balance_update':
                                market.auth.send_db(['collatoral'], data)
                            elif data['type'] == 'auth_success':
                                pass
                            elif data['type'] == 'contact_connected':
                                pass
                            elif data['type'] == 'contact_disconnected':
                                pass
                            else:
                                logging.warning('type?' +  data['type'] + ' ' + str(data))
                    logging.warning('ledgerx ws stopped')
            except (asyncio.TimeoutError) as e:
                logging.warning('ledgerx ws timeout ' + str(e))
            except Exception as e:
                logging.warning('ledgerx ex: ' + str(e))
            finally:
                self.live = False
                logging.info('ledgerx ws ended')
            await asyncio.sleep(2)

class order(object):
    auth = None
    orders = {}
    url = 'https://' + baseurl + '/orders'
    loop = None
    status = True
    STATUS_INSERTED = 200
    STATUS_FILLED = 201
    STATUS_NOFILL = 202
    STATUS_CANCELLED = 203

    @staticmethod
    async def on_status(x):
        logging.info('quoting.status ' + str(x))
        order.status = x['status']
        if not order.status:
            await twosided.cancel()

    @staticmethod
    def factory(ctr, msg):
        if msg['mid'] in order.orders:
            return order.orders[msg['mid']]
        else:
            if msg['cid'] == order.auth.cid:
                if 'contract_id' in msg and 'is_ask' in msg:
                    id = msg['contract_id']
                    ask = msg['is_ask']
                    if id in twosided.orders:
                        ord = twosided.orders[id][ask]
                        order.orders[msg['mid']] = ord
                        ord.mid = msg['mid']
                        logging.info('set order ' + str(ord.inflight) + ' '+ str(msg))
                        return ord
            x = order(ctr, msg)
            logging.warning('new order ' +  str(msg))
            return x

    def __init__(self, contr, msg = None):
        self.contract = contr
        self.inflight = False
        self.status = None
        if msg is not None:
            self.msg = msg
            self.mid = msg['mid']
            self.status = msg['status_type']
            order.orders[self.mid] = self
        else:
            self.mid = None
            self.msg = None
        self.nmid = None
        self.quoting = False # only set the ones constructed in twosided to True, so we can pick up orphaned orders
        self.req = None

    def action(self, msg):
        self.msg = msg
        self.status = msg['status_type']
        if 'filled_size' in msg and msg['filled_size'] != 0:
            try:
                logging.info('fill ' + str(self.msg))
                msg['BTCUSD'] = twosided.last_spot
                # put in last spot price for each fill, option or DAS
                order.auth.send_db(['fills', 'capped'], msg)
                if msg['filled_size'] != msg['original_size']:
                    self.inflight = True
                    asyncio.ensure_future(self.cancel())
                else:
                    self.mid = None # order was fully filled, can not edit order anymore, send out a new order next time.
            except Exception as e:
                logging.warning('action ex ' + str(e) + ' ' + str(msg))
            finally:
                asyncio.ensure_future(self.cancel())
                self.contract.twosided.on_fill(msg)

    async def cancel(self):
        if self.mid is None:
            self.inflight = False
            return
        logging.info('cancel ' + str(self.contract) + ' ' + str(self.mid))
        try:
            mid = self.mid
            self.mid = None # if cancel fails this order will be taken out by state/timer loop below
            with async_timeout.timeout(5, loop=order.loop):
                async with order.auth.session.delete(order.url+'/'+mid, params={'contract_id': self.contract.id, 'token': order.auth.token}) as req:
                     asyncio.ensure_future(req.json())
        except (asyncio.TimeoutError) as e:
            logging.warning('ledx cancel timeout ' + str(e) + ' ' + str(mid))
        except (aiohttp.ClientError) as e:
            logging.warning('ledx cancel ex ' + str(e) + ' ' + str(mid))
        finally:
            self.inflight = False

    async def edit(self, sz, pr):
        self.nmid = None
        pr_cents = order.cents(pr) if sz>0 else order.cents(pr+0.25)
        if pr_cents <= 0 or sz == 0:
            return await self.cancel()
        mid = self.mid
        try:
            with async_timeout.timeout(5, loop=order.loop):
                async with order.auth.session.post(order.url+'/'+self.mid+'/edit', params={'token': order.auth.token,
                                                                    'size': abs(sz),
                                                                    'confirmed_reasonable': 'true',
                                                                    'is_ask': 'false' if sz > 0 else 'true',
                                                                    'price': pr_cents,
                                                                    'contract_id': self.contract.id}) as self.req:
                    try:
                        data = await self.req.text()
                        resp = ujson.loads(data)
                    except:
                        logging.warning('lx edit json ex ' + str(data))
                        return
                    if 'mid' in resp:
                        self.mid = resp['mid']
                        order.orders[self.mid] = self
                    else:
                        logging.warning(str(self.contract) + ' price ' + str(pr_cents) + ' ' + str(resp) + ' ' + str(mid))
                        '''
                            {'error': {'code': 607, 'message': 'possible wash trade denied'}}
                            {'error': {'message': 'insufficient collateral', 'code': 609}}
                            {'error': {'code': 601, 'message': 'unable to find entry'}}
                        '''
                        if 'error' in resp:
                            error = resp['error']
                            if 'code' not in error:
                                self.mid = None
                            elif error['code'] in set([607, 609]):
                                await self.cancel()
                            elif error['code'] == 601:
                                self.mid = None
                    return resp
        except (asyncio.TimeoutError) as e:
            logging.warning('ledx edit timeout ' + str(e) + ' ' + str(self.mid))
        except (aiohttp.ClientError) as e:
            logging.warning('ledx edit ex ' + str(e) + ' ' + str(self.mid))
        finally:
            self.inflight = False
            if self.mid != mid:
                logging.info('edit ' + str(self.contract) + ' ' + str(sz) + ' ' + str(pr) + ' ' + str(pr_cents) + ' mid: ' + str(mid) + '->' + str(self.mid))
            else:
                logging.warning('edit fail ' + str(self.contract) + ' ' + str(sz) + ' ' + str(pr) + ' ' + str(pr_cents) + ' mid: ' + str(mid) + ' ' + str(self.mid) + ' ' + str(self.req.status))

    @staticmethod
    def cents(x):
        return int(25 * ((400 * x) // 100))

    @staticmethod
    async def delete():
        async with order.auth.session.delete(order.url, data={'token': order.auth.token}) as req:
            asyncio.ensure_future(req.json())

    async def send(self, sz, pr):
        if self.inflight or not order.status:
            return
        self.inflight = True
        if self.mid is not None:
            return await self.edit(sz, pr)
        self.nmid = None
        pr_cents = order.cents(pr) if sz > 0 else order.cents(pr + 0.25)
        if pr_cents <= 0 or sz == 0:
            return await self.cancel()
        try:
            with async_timeout.timeout(5, loop=order.loop):
                async with order.auth.session.post(order.url, data={'token': order.auth.token, 'confirmed_reasonable': True,
                                                                    'size': abs(sz),
                                                                    'is_ask': False if sz > 0 else True,
                                                                    'swap_purpose': 'undisclosed', 'price': pr_cents,
                                                                    'contract_id': self.contract.id, 'order_type': 'limit'}) as req:
                    try:
                        resp = await req.json()
                    except:
                        logging.warning('lx send json ex ')
                        return
                    if 'mid' in resp:
                        self.mid = resp['mid']
                        order.orders[self.mid] = self
                    else:
                        logging.warning(str(self.contract) + ' price ' + str(pr_cents) + ' ' + str(resp))
                    return resp
        except (asyncio.TimeoutError) as e:
            logging.warning('ledx send timeout ' + str(e))
        except (aiohttp.ClientError) as e:
            logging.warning('ledx send ex ' + str(e))
        finally:
            self.inflight = False
            if self.mid is not None:
                logging.info('send ' + str(self.contract) + ' ' + str(sz) + ' ' + str(pr) + ' ' + str(pr_cents) + ' ' + str(self.mid))
            else:
                logging.warning('send fail ' + str(self.contract) + ' ' + str(sz) + ' ' + str(pr) + ' ' + str(pr_cents) + ' ' + str(self.mid))

    @staticmethod
    async def state():
        url = 'https://' + baseurl + '/state'
        try:
            with async_timeout.timeout(5, loop=order.loop):
                async with order.auth.session.get(url, params={'token': order.auth.token}) as req:
                    try:
                        resp = await req.json()
                    except:
                        logging.warning('lx state json ex')
                        return
                    return resp
        except (asyncio.TimeoutError) as e:
            logging.warning('ledgerx state timeout ' + str(e))
        except (aiohttp.ClientError) as e:
            logging.warning('ledgerx state client ex ' + str(e))
        except Exception as e:
            logging.warning('ledgerx state ex: ' + str(e))


    @staticmethod
    async def timer(t):
        while not sbreak:
            await asyncio.sleep(t)
            contract.labels['DAS'].twosided.espread *= 0.75
            resp = await order.state()
            if resp is None or 'data' not in resp or 'open_orders' not in resp['data']:
                continue
            t1 = dt.datetime.now()
            logging.info('checking open orders ' + str(len(resp['data']['open_orders'])) + ' espread=' + str(contract.labels['DAS'].twosided.espread))
            logging.info('last hb: ' + str(market.hb))
            for x in resp['data']['open_orders']:
                mid = x['mid']
                if x['cid'] != order.auth.cid or (mid in order.orders and mid == order.orders[mid].mid and order.orders[mid].quoting):
                    continue
                else:
                    t0 = dt.datetime.fromtimestamp(x['inserted_time']/1.e9) #: 1510861775005852625
                    if t1-t0 < dt.timedelta(seconds=10): continue
                    try:
                        with async_timeout.timeout(5, loop=order.loop):
                            async with order.auth.session.delete(order.url+'/'+mid, params={'contract_id': x['contract_id'], 'token': order.auth.token}) as req:
                                asyncio.ensure_future(req.json())
                    except (asyncio.TimeoutError) as e:
                        logging.warning('ledx cancel mid timeout ' + str(e) + ' ' + str(x))
                    except (aiohttp.ClientError) as e:
                        logging.warning('ledx cancel mid ex ' + str(e) + ' ' + str(x))
                    finally:
                        logging.warning('cancel open order unknown ' + str(x))
        logging.info('ledgerx order check timer exit')
        await twosided.cancel()


class twosided(object):
    Out = {}
    market = None
    lastb = None
    lasta = None
    last_spot = None
    orders = {}
    pstatus = param.param('quoting.status', order.on_status)

    def __init__(self, ctr):
        self.contract = ctr
        self.bspread = 500.
        self.aspread = 500.
        self.espread = 0.
        self.bsize = 2
        self.asize = 2
        self.lastac = None
        self.lastbc = None
        self.lastao = order(ctr)
        self.lastbo = order(ctr)
        self.lastao.quoting = True
        self.lastbo.quoting = True
        self.ok = True
        self.option = None
        twosided.Out[ctr] = self
        twosided.orders[ctr.id] = {True: self.lastao, False: self.lastbo} # 'is_ask': True is an offer

    @staticmethod
    async def cancel():
        for i,j in twosided.Out.items():
            await j.lastao.cancel()
            await j.lastbo.cancel()
            j.lastac = None
            j.lastbc = None

    async def canceltwo(self):
        await self.lastao.cancel()
        await self.lastbo.cancel()
        self.lastac = None
        self.lastbc = None

    def on_fill(self, msg):
        self.lastac = None
        self.lastbc = None
        if self.option is not None:
            self.option.on_fill(msg)

    async def on_spot(self, prb, pra=None):
        if pra is None:
            if prb is None: return
            pra = prb
        twosided.lastb = prb
        twosided.lasta = pra
        twosided.last_spot = (prb+pra)/2.

    async def on_quote(self, prb, pra):
        self.ok = False
        prbc = order.cents(prb)
        prac = order.cents(pra+0.25)
        if prbc != self.lastbc and prac != self.lastac:
            bf = prbc < self.lastbc if self.lastbc is not None else True
            self.lastbc = prbc
            self.lastac = prac
            if bf:
                res = await self.lastbo.send(self.bsize, prb)
                res = await self.lastao.send(-self.asize, pra)
            else:
                res = await self.lastao.send(-self.asize, pra)
                res = await self.lastbo.send(self.bsize, prb)
            self.ok = True
            return
        if prbc != self.lastbc:
            self.lastbc = prbc
            res = await self.lastbo.send(self.bsize, prb)
        elif prac != self.lastac:
            self.lastac = prac
            res = await self.lastao.send(-self.asize, pra)
        self.ok = True

    async def on_spread(self, cmd):
        logging.info('spread update '+  str(cmd))
        if 'bspread' in cmd: self.bspread = float(cmd['bspread'])
        if 'bsize' in cmd: self.bsize = int(cmd['bsize'])
        if 'aspread' in cmd: self.aspread = float(cmd['aspread'])
        if 'asize' in cmd: self.asize = int(cmd['asize'])
        self.espread = 0

class dayswap(twosided):
    def __init__(self, ctr):
        super().__init__(ctr)
        self.bspread = 20
        self.aspread = 20
        self.eblean = 0
        self.ealean = 0
        self.olean = 0 # used in b,a for option spot calculation. OptionSpot = (b+a)/2+olean
        self.par = param.param('spread.DAS', self.on_spread)

    async def on_spread(self, cmd):
        self.eblean = 0
        self.ealean = 0
        if 'olean' in cmd: self.olean = float(cmd['olean'])
        await super().on_spread(cmd)

    def on_fill(self, msg):
        self.espread += max(0.02*self.last_spot, 20.)
        if msg['is_ask']:
            self.ealean += 25
            self.asize = max(self.asize-1,1)
        else:
            self.eblean += 25
            self.bsize = max(self.bsize-1,1)

    async def on_spot(self, prb, pra=None):
        await super().on_spot(prb, pra)
        if not self.ok or not twosided.market.live: return
        if pra is None: pra = prb
        prb = prb-self.bspread - self.espread - self.eblean
        pra = pra+self.aspread + self.espread + self.ealean
        await self.on_quote(prb, pra)


async def main1(session, db=False, Fc=None):
    q = login(db=db)
    order.auth = q
    market.auth = q
    m = market()
    twosided.market = m
    await asyncio.wait([q.auth(session)])
    logging.info('completed auth')
    await asyncio.wait([q.me()])
    await asyncio.wait([q.contracts()])
    logging.info('completed contracts')
    if Fc is not None:
        Fc()
        logging.info('completed Fcontracts')
    await asyncio.wait([q.orders()])
    logging.info('completed outstanding orders')
    c = param.param.coros()
    await asyncio.wait([m.start(), order.timer(60)] + c)

