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

sql = 'create table q3(orguserid INT(11) UNSIGNED, userid varchar(65035)) PARTITION BY HASH(orguserid) PARTITIONS 11;'
cur.execute(sql)
db.commit()

subprocess.call(['mysqlimport', '-uroot', '--fields-terminated-by=,', '--lines-terminated-by=*', '--local', '--lock-tables', 'cloud', 'q3.csv'])
sql = 'create index INDEXQ3 on q3 (orguserid);'
cur.execute(sql)
db.commit()
