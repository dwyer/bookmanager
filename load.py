#!/usr/bin/env python3

import os

import dbf3
import tables


class Database(dbf3.ReprMixin):

    def __init__(self, filenames, dirname=None):
        self.tables = {}
        for filename in tables.filenames:
            if dirname is not None:
                path = os.path.join(dirname, filename)
            else:
                path = filename
            name, _ = os.path.splitext(os.path.basename(filename))
            with open(path, 'rb') as fp:
                table = dbf3.Table(fp)
                self.tables[name] = table

    def __getitem__(self, key):
        return self.tables[key]

database = Database(tables.filenames, 'Backup')
for row in database['DATCUST']:
    print(row)
