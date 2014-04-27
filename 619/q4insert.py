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

#sql = 'drop table q4'
#cur.execute(sql)
sql = 'create table q4(ttime BIGINT, txt varchar(65035)) PARTITION BY HASH(ttime) PARTITIONS 101;'
cur.execute(sql)
db.commit()

k = 0
fin = open('q4.csv', 'r')
for line in fin:
  k += 1
  ttime, txt = line.split('\t', 1)
  txt = txt.replace(chr(0), '\n')

  cur.execute('insert into q4 values(%s, %s)',(ttime, txt))
  
  if k % 1000 == 0:
    db.commit()

db.commit()

#subprocess.call(['mysqlimport', '-uroot', '--fields-terminated-by=,', '--lines-terminated-by=\n', '--local', '--lock-tables', 'cloud', 'q4.csv'])
sql = 'create index INDEXQ4 on q4 (ttime);'
cur.execute(sql)
db.commit()
