# ledgerx

Example of python trading api code to quote on LedgerX

Prerequisites:

    python >= 3.6.4

    Example code assumes you run a MongoDB server

    mongo>use ledgerx
          db.createCollection("capped", { capped : true, size : 20000000} )

    mongo>use connections-test
          db.keys.insert_one({'ledgerx-token': 'xxxxxxxxxx'})
          db.keys.insert_one({'mpid': your lx market participant id})
          db.keys.insert_one({'cid': your client id})
          db.keys.insert_one({'ledgerx-baseurl': 'test.ledgerx.com/api'})
          db.keys.insert_one({'ledgerx-market': 'ledgerx-test'}) # db name for logging


## Installation

`virtualvenv venv -p python3 && source venv/bin/activate`

`pip3 install -r requirements.txt`

## API Documentation

[docs.ledgerx.com](https://docs.ledgerx.com)

Usage:

Trader doc: (https://docs.google.com/document/d/1kZUz1OEkcuUt1eNL67eDr7qqFqAf3eZxiDt3QvTs30Q/edit#)

    in ledgerx dir:

    python3 -m run.run_ledgerx

  
