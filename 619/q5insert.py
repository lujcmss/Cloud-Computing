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

sql = 'create table q5(time BIGINT, tweetid BIGINT, place varchar(255)) PARTITION BY RANGE COLUMNS (place) (PARTITION p0 VALUES LESS THAN (\'a\'), PARTITION p1 VALUES LESS THAN (\'b\'), PARTITION p2 VALUES LESS THAN (\'c\'), PARTITION p3 VALUES LESS THAN (\'d\'), PARTITION p4 VALUES LESS THAN (\'e\'), PARTITION p5 VALUES LESS THAN (\'f\'), PARTITION p6 VALUES LESS THAN (\'g\'), PARTITION p7 VALUES LESS THAN (\'h\'), PARTITION p8 VALUES LESS THAN (\'i\'), PARTITION p9 VALUES LESS THAN (\'j\'), PARTITION p10 VALUES LESS THAN (\'k\'), PARTITION p11 VALUES LESS THAN (\'l\'), PARTITION p12 VALUES LESS THAN (\'m\'), PARTITION p13 VALUES LESS THAN (\'n\'), PARTITION p14 VALUES LESS THAN (\'o\'), PARTITION p15 VALUES LESS THAN (\'p\'), PARTITION p16 VALUES LESS THAN (\'q\'), PARTITION p17 VALUES LESS THAN (\'r\'), PARTITION p18 VALUES LESS THAN (\'s\'), PARTITION p19 VALUES LESS THAN (\'t\'), PARTITION p20 VALUES LESS THAN (\'u\'), PARTITION p21 VALUES LESS THAN (\'v\'), PARTITION p22 VALUES LESS THAN (\'w\'), PARTITION p23 VALUES LESS THAN (\'x\'), PARTITION p24 VALUES LESS THAN (\'y\'), PARTITION p25 VALUES LESS THAN (MAXVALUE));'

cur.execute(sql)
db.commit()

subprocess.call(['mysqlimport', '-uroot', '--fields-terminated-by=,', '--lines-terminated-by=\n', '--local', '--lock-tables', 'cloud', 'q5.csv'])
sql = 'create index INDEXQ5 on q5 (place, time);'
cur.execute(sql)
db.commit()
