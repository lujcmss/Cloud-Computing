#!/usr/bin/python

import json
import sys
import codecs
import time
import calendar
import locale

month = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06',
     'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
#sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)

for line in sys.stdin:
    data = json.loads(line)
    # get date time
    date_str = data['created_at'].split(' ')
    date = date_str[5] + '-' + month[date_str[1]] + '-' + date_str[2] + '+' + date_str[3]
    time_in_sec = calendar.timegm(time.strptime(date, '%Y-%m-%d+%H:%M:%S'))
    time_in_sec = str(time_in_sec).encode('utf-8')
    # tw_id
    tw_id = data['id_str'].encode('utf-8')
    # process text
    text = chr(0).join(data['text'].splitlines())
    sys.stdout.write(str(time_in_sec) + u'000' + u'*' + tw_id + u'\t' + text + u'\n')
