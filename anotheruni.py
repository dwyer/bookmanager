#!/usr/bin/env python3

import sqlite3
import datetime
import sys
import csv

Q = """

select DISTINCT datcust.*
from datcust join DATPOS join DATSTOCK
where DATCUST.SHIPTO = DatPOS.SHIPTO
and DATPOS.ISBN = DATSTOCK.ISBN
and datpos.SHIPTO != ''
and (
DATSTOCK.CAT = 'TOYS'
or DATSTOCK.CAT = 'GAMES'
or DAtstock.cat = 'GRAPHIC'
or datstock.cat = 'FICTION' and (datstock.cat2 = 'SCIFI' or datstock.cat2 = 'FANTASY')
)

"""

with open('anotheruni.csv', 'w') as fp:
    writer = csv.writer(fp)
    with sqlite3.connect('test.db') as conn:
        cur = conn.cursor()
        for row in cur.execute(Q):
            writer.writerow(row[1:])
            print(row)
