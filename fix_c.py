#!/usr/bin/env python3

import csv
import sqlite3

header = [
    'CUSTKEY', 'CLOKA', 'SHIPTO', 'BILLTO', 'SHOP', 'NAME', 'ADD1', 'ADD2',
    'CITY', 'PROV', 'PCODE', 'COUNTRY', 'EMAIL', 'PHONE', 'PHONE2', 'FAX',
    'CREDITCARD', 'CONTACT', 'SHIPVIA', 'TERMS', 'DISC', 'BO', 'CANCEL',
    'NOTES', 'KEYNUM', 'INT1', 'INT2', 'INT3', 'INT4', 'SAN', 'LASTORDER',
    'SALESTHIS', 'SALESLAST', 'SALES3', 'FREQBUYER', 'POINTS', 'FBRECIP',
    'REP', 'ENTDATE', 'TAXCODE', 'TX1', 'TX2', 'GETDEPOSIT', 'CREDITLIM',
    'OVERDUE', 'COMBINEPO', 'EDICODES', 'WEBACCESS', 'COMM_PREF']

conn = sqlite3.connect('test.db')
cur = conn.cursor()
s = set()
with open('cust_A.csv') as c0, open('cust_B.csv') as c1, open('cust_C2.csv', 'w') as c2:
    r0 = csv.DictReader(c0)
    r1 = csv.DictReader(c1)
    r2 = csv.writer(c2)
    r2.writerow(header)
    i  = 0
    for table in [r0, r1]:
        for row in table:
            s.add(row['CUSTKEY'])
            i += 1
    print(i)
    print(len(s))
    for row in cur.execute('select * from datcust', []):
        if repr(row[0]) not in s:
            r2.writerow(row)
