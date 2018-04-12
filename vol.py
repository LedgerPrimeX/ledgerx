import re, pymongo, param, key, dateutil.parser, collections, datetime as dt, ast
import gnureadline

gnureadline.parse_and_bind('tab: complete')
gnureadline.parse_and_bind('set editing-mode vi')

client = pymongo.MongoClient()
db = client[key.keys['ledgerx-order']]
contracts = {}
contractsAll = {}
now = dt.datetime.utcnow()
for x in db['contracts'].find({'contracts':{'$exists': True}}): #.sort('$natural', -1):
    contracts.update({i['id']:i for i in x['contracts'] if dateutil.parser.parse(i['date_expires']).replace(tzinfo=None) > now})
    contractsAll.update({i['id']:i for i in x['contracts']})

cKeys = set(contracts.keys())

Exps = collections.defaultdict(list)
Labels = {}
Ids = {}
for i, j in contracts.items():
    if j['derivative_type'] == 'options_contract':
        Ids[j['label']] = i
        Labels[i] = j['label']
        exp = dateutil.parser.parse(j['date_expires']).replace(tzinfo=None).date()
        Exps[exp].append(j)

NamesS = set(Ids.keys())
ExpS = set([i.isoformat() for i in Exps.keys()])

p = param.param('option.vol')

rex1 = re.compile('(\d+[\.]?\d*)')
rex2 = re.compile('([+-]?\d+[\.]?\d*),([+-]?\d+[\.]?\d*)')
rex3 = re.compile('(\d+),(\d+)')
rex1 = re.compile('(\d+[\.]?\d*)')
rex1n = re.compile('([+-]?\d+[\.]?\d*)')
rexb = re.compile('True|False')
rexday = re.compile('(BTC|ETH)-Day$')
rexdayK = re.compile('(BTC|ETH)-Day-(Call|Put)-\$([0-9,]*)$')

def getSize(x):
    name = input("What's your " + x + " bsize/asize? ")
    res = rex3.match(name)
    if res:
        cbs = res.groups()[0]
        cas = res.groups()[1]
        return int(cbs),int(cas)
    else:
        return None


def getVal(x, y='vol'):
    name = input("What's your " + x + " " + y + "? ")
    res = rex1n.match(name)
    if res:
        v = float(res.groups()[0])
        if y == 'vol' and (v<0.5 or v > 1.5):
            name = input("This vol " + str(v) + " seems low/high, (try decimal in range like 0.5-1.5) sure? ")
            if name != 'y':
                return getVal(x)
        if y == 'delta vol' and (abs(v) > 0.2):
            name = input("This delta vol " + str(v) + " seems high, (try like 0.02) sure? ")
            if name != 'y':
                return getVal(x)
        if y == 'mult lean' and v<1:
            print('mult lean has to be >= 1')
            return None
        return v
    else:
        return None

def getStatus(x):
    val = input('status ' + x + ' ? ')
    if rexb.match(val):
        x = ast.literal_eval(val)
        return x
    else:
        return None

def getLean(x):
    name = input("What's your " + x + " blean/alean? ")
    res = rex2.match(name)
    if res:
        v0 = res.groups()[0]
        v1 = res.groups()[1]
        return float(v0), float(v1)
    else:
        return None

def getName():
    name = input("What's your option? ")
    if name == '': return None
    if name == 'q': exit()
    if name not in NamesS and name not in ExpS:
        if rexday.match(name) or rexdayK.match(name):
            return name
        y = input('unknown name '+ name + ' try '+ list(NamesS)[0]+ ' or ' + str(list(Exps)[0])+ ' sure? ')
        if y != 'y':
            return getName()
    return name

def keyK(k1):
    k1 = k1[1]
    r = k1.rfind(' ')
    k = float(k1[r+2:].replace(',',''))
    return (k1[:r], k)

p0 = p.get()

if 'call' in p0:
    print('call', p0['call'])
if 'put' in p0:
    print('put', p0['put'])
for i in sorted(ExpS):
    if i in p0:
        print(i, p0[i])
for i,j in sorted(Labels.items(),key=keyK):
    if str(i) in p0:
        print(str(i), Labels[i], p0[str(i)])
for i,j in p0.items():
    if 'Day' in i:
        print(i,j)

# prepolulate the readline history, so up arrow gives something
for i,j in sorted(Labels.items(),key=keyK, reverse=True):
    gnureadline.add_history(j)
for i in sorted(ExpS):
    gnureadline.add_history(i)



while True:
    # to enable new expiration:
    #  p0['2018-01-19'] = {'status':True}
# p0['2018-01-26'] = {'vmult': 3.5, 'asize': 1, 'round': 1.0, 'blean': 500, 'alean': 500, 'dvol': 0.0, 'bsize': 1, 'status': True}
    option = getName()
    p0 = p.get()
    if option is None:
        vol = getVal('call')
        if vol is not None:
            p0['call'].update({'vol':vol})
        vol = getVal('put')
        if vol is not None:
            p0['put'].update({'vol':vol})

        x = getSize('call')
        if x is not None:
            p0['call'].update({'bsize': x[0], 'asize' : x[1]})
        x = getSize('put')
        if x is not None:
            p0['put'].update({'bsize': x[0], 'asize': x[1]})
        print(p0)
        x = input('ok? ')
        if x == 'y':
            p.send(p0)
    else:
        id = str(Ids[option]) if option in NamesS else option #numeric LX option id, or exp date, or 'BTC-Day' or 'BTC-Day-Call-$8000' etc.
        if id in p0:
            p1 = dict(p0[id])
            print('current: ', id, p1)
        else:
            p1 = {'name': option}
            print('no current setting ', id, p1)
        if option in ExpS or rexday.match(option):
            dvol = getVal(option, 'delta vol')
            dcvol = getVal(option, 'delta cvol')
            dpvol = getVal(option, 'delta pvol')
            mlean = getVal(option, 'mult lean')
        else:
            dvol = None
            dcvol = None
            dpvol = None
            mlean = None
        vol = getVal(option)
        lean = getLean(option)
        vmult = getVal(option, 'vega mult')
        rnd = getVal(option, 'round')
        sz = getSize(option)
        stat = getStatus(option)
        if dvol is not None:
            p1.update({'dvol': dvol})
        if dcvol is not None:
            p1.update({'dcvol': dcvol})
        if dpvol is not None:
            p1.update({'dpvol': dpvol})
        if vol is not None:
            p1.update({'vol': vol})
        if vmult is not None:
            p1.update({'vmult': vmult})
        if lean is not None:
            p1.update({'blean':lean[0], 'alean': lean[1]})
        if mlean is not None:
            p1.update({'mlean': mlean})
        if rnd is not None:
            if rnd < 0.5 and 'round' in p1[id]:
                p1.pop('round')
            else:
                p1.update({'round' : rnd})
        if sz is not None:
            p1.update({'bsize': sz[0], 'asize' : sz[1]})
        if stat is not None:
            # drop at option level if true, and honor exp level status
            if stat and option in NamesS and 'status' in p1:
                p1.pop('status')
                print('dropped status from option, honoring exp level now')
            else:
                p1.update({'status' : stat})
        print('new: ', id, p1)
        x = input('ok? ')
        if x == 'y':
            p0.update({id: p1})
            p2 = p0.copy()
            for i in p0.keys():
                if i.isdigit() and int(i) not in cKeys:
                    print('pop', i, contractsAll[int(i)] if int(i) in contractsAll else None)
                    p2.pop(i)
            p.send(p2)

