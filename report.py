#!/usr/bin/env python3

import sqlite3
import datetime
import sys

q = """
select DATPOS.DATE, DATPOS.time, DATAR.INVOICE, DATAR.SALENET
from DATAR join DATPOS
where DATAR.INVOICE = DATPOS.INVOICE and DATPOS.PRICE < 0;
"""

table = [[0] * 7 for _ in range(24)]
t = 0

best = 0

with sqlite3.connect('test.db') as conn:
    cur = conn.cursor()
    # for row in cur.execute('select DATE, TIME as net from DATPOS'):
    #     print(row)
    for date, time, inv, net in cur.execute(q):
        dt = datetime.datetime.strptime(date + time, '%Y-%m-%d%H:%M')
        wd = dt.date().weekday()
        if net:
            t += net
            table[dt.hour][wd] += net
            if best < table[dt.hour][wd]:
                best = table[dt.hour][wd]

dow = 'M T W Th F S Su'.split()

print('<table border="1">')
print('<tr>')
print('<td></td>')
for d in dow:
    print('<td>%s</td>' % d)
print('</tr>')
for i, hour in enumerate(table):
    print('<tr>')
    print('<td style="text-align:right">%s</td>' % datetime.time(hour=i).strftime('%-I %p'))
    for j, total in enumerate(hour):
        if total:
            rat = total / best * 100
            color = 'yellow'
            if rat > 50:
                color = 'green'
            elif rat < 10:
                color = 'red'
            print(
                '<td style="text-align:right; background-color: %s">%.02f</td>' % (
                    color, total))
        else:
            print('<td></td>')
    print('</tr>')
print('</table>')
