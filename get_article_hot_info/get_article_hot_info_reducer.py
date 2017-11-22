#coding=gbk
#by yj,2017.9.5

import sys

lastnum=0
laststr=''
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    if line==laststr:
        lastnum+=1
    else:
        if laststr!='':
            print laststr+'\t'+str(lastnum)
        laststr=line
        lastnum=1
print laststr+'\t'+str(lastnum)
