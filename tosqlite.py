#!/usr/bin/env python3

import dbf3

import base64
import datetime
import decimal
import os
import sqlite3
import sys

filenames = """
DATAR.DBF
DATARREC.dbf
DATCAT.DBF
DATCFILT.DBF
DATCUST.DBF
DATDEFLT.DBF
DATGLSYN.DBF
DATINVCS.DBF
DATLOG.dbf
DATMEMO.DBF
DATNOTES.DBF
DATORDER.DBF
DATPASS.DBF
DATPO.DBF
DATPOS.DBF
DATRETRN.DBF
DATRPT.DBF
DATSCHED.DBF
DATSTATS.DBF
DATSTOCK.DBF
DATSUPLR.DBF
DATTXGRP.DBF
DATTXTYP.DBF
DATUSERS.DBF
""".strip().split()
# DATWLQUE.DBF

def create_table(table, cursor):
    c = cursor

dirname = sys.argv[1]
dbname = 'test.db'
os.remove(dbname)
with sqlite3.connect(dbname) as con:
    c = con.cursor()
    for filename in filenames:
        path = os.path.join(dirname, filename)
        print(path)
        with open(path, 'rb') as fp:
            table = dbf3.Table(fp)
            lst = []
            field_names = []
            for field in table.fields:
                if field.type in 'C':
                    sql_type = 'varchar(%d) not null' % field.length
                elif field.type in 'D':
                    sql_type = 'date'
                elif field.type in 'L':
                    sql_type = 'boolean'
                elif field.type in 'M':
                    sql_type = 'integer not null'
                elif field.type in 'N':
                    if field.num_decimals:
                        sql_type = 'decimal(%d,%d)' % (field.length-1,
                                                       field.num_decimals)
                    else:
                        sql_type = 'integer'
                else:
                    raise Exception(field.type)
                lst.append('%s %s' % (repr(field.name), sql_type))
                field_names.append(repr(field.name))
            table_name = repr(os.path.splitext(filename)[0])
            cmd = 'create table %s (%s);' % (table_name, ', '.join(lst))
            # print(cmd)
            c.execute(cmd)
            field_names_str = ','.join(field_names)
            qs_str = ','.join('?' * len(field_names))
            for record in table:
                row = []
                for value in record:
                    if isinstance(value, (decimal.Decimal)):
                        value = float(value)
                    elif isinstance(value, datetime.date):
                        value = str(value)
                    row.append(value)
                cmd = 'insert into %s(%s) values (%s);' % (
                    table_name,
                    field_names_str,
                    qs_str,
                )
                try:
                    c.execute(cmd, row)
                except sqlite3.IntegrityError:
                    print(cmd, row)
