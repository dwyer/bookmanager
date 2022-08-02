#!/usr/bin/env python
# -*-coding:utf-8-*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys
import sqlite3
import csv

with open('cust.csv', 'w') as fp:
    writer = csv.writer(fp)
    writer.writerow(['name', 'email', 'phone', 'repeat'])
    with sqlite3.connect(sys.argv[1]) as connect:
        c = connect.cursor()
        for row in c.execute("""
    select name, email, phone, lastorder != entdate from datcust where email != '' or phone != ''
        """):
            writer.writerow(row)
