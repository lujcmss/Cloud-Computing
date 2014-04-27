#!/usr/bin/python

import sys
import json

def main(argv):
	userid = None
	cnt = 0

	for line in sys.stdin:
		curr_userid, num_str = line.split('\t')
		curr_cnt = int(num_str)
		if userid is None:
			userid = curr_userid
			cnt = curr_cnt
		elif curr_userid == userid:
			cnt += curr_cnt
		else:
			print userid + '\t' + str(cnt)
			userid = curr_userid
			cnt = curr_cnt

	if userid is not None:
		print userid + '\t' + str(cnt)

if __name__ == "__main__":
  main(sys.argv)