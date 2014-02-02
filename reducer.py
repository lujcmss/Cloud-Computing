#!/usr/bin/python

import sys
import re

alldate = {'20130601':0, '20130602':0, '20130603':0, '20130604':0, '20130605':0, '20130606':0, '20130607':0, '20130608':0, '20130609':0, '20130610':0, '20130611':0, '20130612':0, '20130613':0, '20130614':0, '20130615':0, '20130616':0, '20130617':0, '20130618':0, '20130619':0, '20130620':0, '20130621':0, '20130622':0, '20130623':0, '20130624':0, '20130625':0, '20130626':0, '20130627':0, '20130628':0, '20130629':0, '20130630':0}

def main(argv):
  current_word = None
  current_count = 0

  for line in sys.stdin:
    word, datencount = line.split('\t')
    date = datencount[:8]
    count = datencount[8:]

    try:
      count = int(count)
    except ValueError:
      continue

    if current_word == word:
      current_count += count
      if date in alldate:
        alldate[date] += count
    else:
      if current_word and current_count > 100000:
        s = ""
        s += str(current_count) + '\t' + current_word + '\t'
        for i in sorted(alldate.keys()):
          s += i + ":" + str(alldate[i]) + '\t'
        print s
      for i in alldate.keys():
        alldate[i] = 0
      if date in alldate:
        alldate[date] += count
      current_count = count
      current_word = word

  if current_word == word and current_count > 100000:
    s = ""
    s += str(current_count) + '\t' + current_word + '\t'
    for i in sorted(alldate.keys()):
      s += i + ":" + str(alldate[i]) + '\t'
      alldate[i] = 0
    print s

if __name__ == "__main__":
  main(sys.argv)
