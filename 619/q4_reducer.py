#!/usr/bin/python

import sys
import locale
import codecs

#sys.stdin = codecs.getreader(locale.getpreferredencoding())(sys.stdin)
#sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)

for line in sys.stdin:
	sys.stdout.write(line)