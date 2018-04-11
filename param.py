import aiohttp, asyncio, pymongo, time, datetime as dt, pandas as pd, logging

'''
Assumes a 'capped' mongo collection. see:

https://docs.mongodb.com/manual/core/capped-collections/

'''

class param():
    client = pymongo.MongoClient()
    db = client['ledgerx']['capped']
    params = {}
    alive = True

    def __init__(self, topic, cb=None):
        self.cb = cb
        self.topic = topic
        self.last = param.params[topic].last if topic in param.params else None
        param.params[topic] = self
        if self.last is not None and self.cb is not None:
            asyncio.ensure_future(self.cb(self.last))

    @staticmethod
    def coros():
        return [param.run()]

    @staticmethod
    async def run():
        cursor = param.db.find({'topic': {"$exists": True}}, cursor_type=pymongo.CursorType.TAILABLE)
        while param.alive and cursor.alive:
            last = set()
            for x in cursor:
                tp = x['topic']
                last.add(tp)
                if tp not in param.params:
                    param(tp)
                param.params[tp].last = x
            for tp in last:
                p = param.params[tp]
                if p.cb is not None:
                    await p.cb(p.last)
            await asyncio.sleep(1)
        logging.info('param exit')

    @staticmethod
    async def stop():
        param.alive = False

    def get(self):
        cs = param.db.find({'topic': self.topic}).sort([('$natural', -1)]).limit(1)
        try:
            x = cs.next()
        except StopIteration:
            return None
        self.id = x.pop('_id')
        self.time = x.pop('time')
        x.pop('topic')
        return x

    def send(self, msg):
        msg['topic'] = self.topic
        msg['time'] = time.time()
        msg.pop('_id', None)
        param.db.insert_one(msg)

