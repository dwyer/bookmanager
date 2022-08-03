#!/usr/bin/env python3

import dbf3

import base64
import datetime
import decimal
import os
import sqlite3
import sys
import zipfile
import re


def dbase_table_to_sqlite_commands(fp, name):
    commands = []
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
            # sql_type = 'integer not null'
            sql_type = 'integer'
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
    table_name = repr(os.path.splitext(name)[0])
    cmd = 'create table %s (%s);' % (table_name, ', '.join(lst))
    commands.append([cmd, None])
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
        commands.append([cmd, row])
    return commands


def zip2sqlite(zipname, dbname):
    try:
        os.remove(dbname)
    except FileNotFoundError:
        pass
    with zipfile.ZipFile(zipname) as zip:
        commands = []
        for name in zip.namelist():
            if not re.search(r'\.dbf$', name, re.I):
                continue
            print(zipname, name)
            with zip.open(name) as fp:
                try:
                    commands.extend(dbase_table_to_sqlite_commands(fp, name))
                except UnicodeDecodeError as e:
                    print(zipname, name, e)
                except AssertionError as e:
                    print(zipname, name, e)
    with sqlite3.connect(dbname) as con, zipfile.ZipFile(zipname) as zip:
        c = con.cursor()
        for cmd, row in commands:
            try:
                c.execute(cmd, row or [])
            except sqlite3.IntegrityError:
                print(cmd, row)
                raise



if __name__ == '__main__':
    zipname = sys.argv[1]
    dbname = 'test.db'
    zip2sqlite(zipname, dbname)
