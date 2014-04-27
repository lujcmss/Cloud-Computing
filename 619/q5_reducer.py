#!/usr/bin/python

import sys

def main(argv):
	text = None
	for line in sys.stdin:
		curr_text = line[:-1]
		if text is None:
			text = curr_text
		elif text == curr_text:
			continue
		else:
			print text
			text = curr_text

	if text is not None:
		print text

if __name__ == "__main__":
	main(sys.argv)