#!/usr/bin/python

import sys
import json

def main(argv):
  	cache = {}
  	pad = ''
  	pad_arr = []
  	for i in range(0, 21):
  		pad_arr.append(pad)
  		pad += '0'

	for line in sys.stdin:
		tweet = json.loads(line)
		userid = tweet['user']['id_str']
		userid = pad_arr[20 - len(userid)] + userid
		if userid in cache:
			cache[userid] = cache[userid] + 1
		else:
			cache[userid] = 1

	for key, value in cache.iteritems():
		print key + '\t' + str(value)

if __name__ == "__main__":
  main(sys.argv)