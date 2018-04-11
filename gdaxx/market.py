import asyncio, aiohttp, async_timeout, requests, logging, datetime as dt, collections, ujson, time

'''
This is just a trivial example

Clearly prod code would have several order of magnitude more code to retrieve robust b/a book from multiple exchanges

The below code does not protect from exchange hickups, etc. etc.

'''

sbreak = False

async def get_last_price(session, F):
    while not sbreak:
        async with session.get('https://api.gdax.com/products/BTC-USD/ticker') as req:
            x = await req.json()
            await F(float(x['bid']),float(x['ask']))
            await asyncio.sleep(5)

