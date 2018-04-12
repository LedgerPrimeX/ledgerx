import re, param
import gnureadline

gnureadline.parse_and_bind('tab: complete')
gnureadline.parse_and_bind('set editing-mode vi')

p = param.param('spread.DAS')

rex2 = re.compile('(\d+)@([+-]?\d+[\.]?\d*),(\d+)@([+-]?\d+[\.]?\d*)')
rex3 = re.compile('(\d+)@([+-]?\d+[\.]?\d*),(\d+)@([+-]?\d+[\.]?\d*),([+-]?\d+[\.]?\d*)')
rex1 = re.compile('(\d+)@([+-]?\d+[\.]?\d*)')
rexf = re.compile('(\d+[\.]?\d*)')

x = p.get()

if x is not None:
   olean = x['olean'] if 'olean' in x else 0

while True:
    x = p.get()
    print(x)
    if x is not None:
        olean = x['olean'] if 'olean' in x else 0
    else:
        olean = 0
    name = input("What's your spread? ")
    res = rex3.match(name)
    if res:
        bsz = int(res.groups()[0])
        bspr = float(res.groups()[1])
        asz = int(res.groups()[2])
        aspr = float(res.groups()[3])
        olean = float(res.groups()[4])
        p.send({'bspread': bspr, 'bsize': bsz, 'aspread': aspr, 'asize': asz, 'olean':olean})
    else:
        res = rex2.match(name)
        if res:
            bsz = int(res.groups()[0])
            bspr = float(res.groups()[1])
            asz = int(res.groups()[2])
            aspr = float(res.groups()[3])
            p.send({'bspread': bspr, 'bsize': bsz, 'aspread': aspr, 'asize': asz, 'olean': olean})
        else:
            res = rex1.match(name)
            if res:
                sz = int(res.groups()[0])
                spr = float(res.groups()[1])
                p.send({'bspread': spr, 'bsize': sz, 'aspread': spr, 'asize': sz, 'olean':olean})
            else:
                print('bad format')
