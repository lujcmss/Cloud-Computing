#!/usr/bin/python

import MySQLdb
import os
import sys
import subprocess

db = MySQLdb.connect(host='localhost',
                     user='root',
                     passwd='',
                     db='cloud')
cur = db.cursor()

sql = 'create table q6(userid INT(11) UNSIGNED, num INT(11), tot INT(11)) PARTITION BY HASH(userid) PARTITIONS 11;'
cur.execute(sql)
db.commit()

subprocess.call(['mysqlimport', '-uroot', '--fields-terminated-by=,', '--lines-terminated-by=\n', '--local', '--lock-tables', 'cloud', 'q6.csv'])
sql = 'create index INDEXQ6 on q6 (userid);'
cur.execute(sql)
db.commit()
