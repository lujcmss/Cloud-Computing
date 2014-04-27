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

sql = 'create table q2(userid INT(11) UNSIGNED, ttime BIGINT, tweetid varchar(65035)) PARTITION BY HASH(userid) PARTITIONS 101;'
cur.execute(sql)
db.commit()

subprocess.call(['mysqlimport', '-uroot', '--fields-terminated-by=,', '--lines-terminated-by=*', '--local', '--lock-tables', 'cloud', 'q2.csv'])
sql = 'create index INDEXQ2 on q2 (userid, ttime);'
cur.execute(sql)
db.commit()
