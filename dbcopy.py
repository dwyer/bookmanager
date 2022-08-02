#!/usr/bin/env python3

import sys

import dbf3

with open(sys.argv[1], 'rb') as fp, open('tmp.dbf', 'wb') as out:
    wrapper = dbf3.Table(fp, out)
    wrapper.writetmp()
