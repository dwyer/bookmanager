#!/usr/bin/env python3

import dbf3

import base64
import datetime
import decimal
import sys

filename = sys.argv[1]
with open(filename, 'rb') as fp:
    db = dbf3.Table(fp)
print("""
<style>
td { white-space: nowrap; }
date { white-space: nowrap; }
.bool { text-align: center; }
.false { text-align: center; color: red; }
.true { text-align: center; color: green; }
.number { text-align: right; }
</style>
""")
print('<h1>%s</h1>' % filename)
print('<table border=1>')
print('<tr>')
for field in db.fields:
    print('<th>%s</th>' % field.name)
print('</tr>')
for record in db.records:
    print('<tr>')
    for value in record.values():
        if value is None:
            print('<td></td>')
        elif value is False:
            print('<td class="bool">✖</td>')
        elif value is True:
            print('<td class="bool">✓</td>')
        elif isinstance(value, bytes):
            print('<td>base64:%s</td>' % base64.b64encode(value).decode())
        elif isinstance(value, (int, decimal.Decimal)):
            print('<td class="number">%s</td>' % value)
        elif isinstance(value, datetime.date):
            print('<td><date>%s</date></td>' % value)
        else:
            print('<td>%s</td>' % value)
    print('</tr>')
print('</table>')
