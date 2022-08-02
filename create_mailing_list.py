#!/usr/bin/env python3

import csv
import sqlite3

Q = """
select -sum(datpos.price) as p, t.*
    from datpos join (
        select count(datpos.date) as visits, datcust.*
            from datpos join datcust
            where datpos.shipto = datcust.shipto
                and datpos.price < 0
                and datpos.date >= '2021-01-01'
            group by datpos.SHIPTO) as t
    where datpos.SHIPTO = t.SHIPTO
        and PRICE < 0
        and visits >= 2
    group by datpos.SHIPTO
    order by p DESC
    limit 1000;
"""

Q = """
select -sum(datpos.price) as p, t.*
    from datpos join (
        select count(datpos.date) as visits, datcust.*
            from datpos inner join datcust
            where datpos.shipto = datcust.shipto
                and datpos.price < 0
                and datpos.date >= '2021-01-01'
            group by datpos.SHIPTO) as t
    where datpos.SHIPTO = t.SHIPTO
        and PRICE < 0
    group by datpos.SHIPTO
    order by p DESC;
"""

header = ['total', 'visits'] + [
    'CUSTKEY', 'CLOKA', 'SHIPTO', 'BILLTO', 'SHOP', 'NAME', 'ADD1', 'ADD2',
    'CITY', 'PROV', 'PCODE', 'COUNTRY', 'EMAIL', 'PHONE', 'PHONE2', 'FAX',
    'CREDITCARD', 'CONTACT', 'SHIPVIA', 'TERMS', 'DISC', 'BO', 'CANCEL',
    'NOTES', 'KEYNUM', 'INT1', 'INT2', 'INT3', 'INT4', 'SAN', 'LASTORDER',
    'SALESTHIS', 'SALESLAST', 'SALES3', 'FREQBUYER', 'POINTS', 'FBRECIP',
    'REP', 'ENTDATE', 'TAXCODE', 'TX1', 'TX2', 'GETDEPOSIT', 'CREDITLIM',
    'OVERDUE', 'COMBINEPO', 'EDICODES', 'WEBACCESS', 'COMM_PREF']

conn = sqlite3.connect('test.db')
cur = conn.cursor()
with open('cust_A.csv', 'w') as c0, open('cust_B.csv', 'w') as c1, open('cust_C.csv', 'w') as c2:
    r0 = csv.writer(c0)
    r1 = csv.writer(c1)
    r2 = csv.writer(c2)
    for r in [r0, r1, r2]:
        r.writerow(header)
    for i, row in enumerate(cur.execute(Q, [])):
        if row[0] > 200 and row[1] >= 2:
            r0.writerow(row)
        elif row[1] >= 2:
            r1.writerow(row)
        else:
            r2.writerow(row)
        # print(i, row)
