#!/usr/bin/python

import sys
import re
import os

topics=["Media:","Special:","Talk:","User:","User_talk:","Project:","Project_talk:","File:","File_talk:","MediaWiki:","MediaWiki_talk:","Template:","Template_talk:","Help:","Help_talk:","Category:","Category_talk:","Portal:","Wikipedia:","Wikipedia_talk:"]
imgs=[".jpg", ".gif", ".png", ".JPG", ".GIF", ".PNG", ".txt", ".ico"]
boilers=["404_error/","Main_Page","Hypertext_Transfer_Protocol","Favicon.ico","Search"]

def main(argv):
  
  for lines in sys.stdin:
    filename = str(os.environ["map_input_file"])
    date = filename.split("-")[2];

    list = lines.split()

    if list[0] != "en":
      continue
  
    if list[1][0].islower():
      continue

    flag = False
    for topic in topics:
      if list[1].startswith(topic):
        flag = True
        break
    if flag:
      continue

    for img in imgs:
      if list[1].endswith(img):
        flag = True
        break
    if flag:
      continue

    for boiler in boilers:
      if list[1] == boiler:
        flag = True
        break
    if flag:
      continue
  
    print list[1] + '\t' + date + list[2]

if __name__ == "__main__":
  main(sys.argv)
