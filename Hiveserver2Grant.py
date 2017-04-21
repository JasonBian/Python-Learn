#!/bin/env python2.7

import sys
import pyhs2

with pyhs2.connect(host='bi-lc-179',
                   port=10000,
                   authMechanism="PLAIN",
                   user='hive',
                   password='hive',
                   database='dw_raw_std') as conn:
    with conn.cursor() as cur:
        # Show databases
        # print cur.getDatabases()

        cur.execute("set role admin")
        cur.execute("show tables")

        # Fetch table results
        if len(sys.argv) == 4:
            tables = (sys.argv[3]).split(',')
            for i in cur.fetch():
                if i[0] in tables:
                    sql = "grant {0} on table {1} to user {2}".format(sys.argv[1], i[0], sys.argv[2])
                    # print sql
                    cur.execute(sql)
        else:
            for i in cur.fetch():
                sql = "grant {0} on table {1} to user {2}".format(sys.argv[1], i[0], sys.argv[2])
                # print sql
                cur.execute(sql)
