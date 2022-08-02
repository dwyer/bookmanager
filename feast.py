#!/usr/bin/env python3

import csv
import sqlite3
import collections

PAYMENT_TYPES = ('Points', 'Visa', 'Masterca')

QUERY = """
select datpos.date, datpos.time, datpos.INVOICE, datpos.ISBN, datpos.price, datpos.DISC from datpos join (select invoice from datpos where isbn = 'A803599700282' and invoice != '000000') as t1 where datpos.invoice = t1.invoice;
"""

def fmt(price):
    return '%.02f' % price

def fmtrow(*args):
    return [fmt(val) if isinstance(val, float) else val for val in args]

dd = collections.defaultdict(list)
with sqlite3.connect('test.db') as conn:
    cur = conn.cursor()
    for date, time, invoice, isbn, price, disc in cur.execute(QUERY):
        key = (date, time, invoice)
        val = (isbn, price, disc)
        dd[key].append(val)

with open('feast-n-fic.csv', 'w') as fp:
    writer = csv.writer(fp)
    writer.writerow(('time', 'invoice', 'feast', 'other', 'total', 'paid'))
    ff_total = 0
    other_total = 0
    total_paid = 0
    count = 0
    for key, vals in dd.items():
        date, time, invoice = key
        feast = 0
        other = 0
        paid = 0
        for isbn, price, disc in vals:
            if disc:
                price = price * (1 - disc/100)
            if isbn.startswith('A8'):
                feast += price
            elif isbn.startswith('a COUNTY'):
                continue
            elif price < 0:
                assert isbn in PAYMENT_TYPES, isbn
                paid += price
            else:
                other += price
        if paid == 0:
            continue
        ff_total += feast
        other_total += other
        total_paid += paid
        total = (feast + other)
        writer.writerow(fmtrow(f'{date} {time}', invoice, feast, other, total, paid))
        count += 1
    total_total = ff_total + other_total
    writer.writerow(fmtrow('Totals', '', ff_total, other_total, total_total,
                           total_paid))
    writer.writerow(fmtrow('Averages', '', ff_total/count, other_total/count,
                           total_total/count, total_paid/count))
