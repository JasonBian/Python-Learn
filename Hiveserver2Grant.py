import pyhs2

with pyhs2.connect(host='bi-02',
                   port=10000,
                   authMechanism="PLAIN",
                   user='hive',
                   password='hive',
                   database='liull') as conn:
    with conn.cursor() as cur:
        cur.execute("set role admin")
        cur.execute("show tables")

        # Fetch table results
        for i in cur.fetch():
            print str(i)
            sql = "grant all on table " + i[0] + " to user hive"
            print sql
            cur.execute(sql)
