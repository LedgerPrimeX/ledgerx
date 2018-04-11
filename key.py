import pymongo

client = pymongo.MongoClient()
db = client['connections']['keys']

def read_keys():
    res = {}
    for x in db.find():
        for i,j in x.items():
            res[i] = j
    return res

keys = read_keys()

