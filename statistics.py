#!/usr/bin/python

import os
import sys

dir = sys.argv[1]

linecount = 0
pop = 0
poparticle = ""
for i in os.listdir(dir):
  if i.startswith('part'):
    file = os.path.join(dir, i)
    fin = open(file, 'r')
    for line in fin:
      linecount += 1
      data = line.split('\t')
      if int(data[0]) > pop:
        pop = int(data[0])
        poparticle = data[1]
      if (data[1] == 'James Gandolfini'):
        print line
      if (data[1] == 'James_Gandolfini'):
        print line

print 'Total lines:', linecount
print 'Most popular:', poparticle, pop
