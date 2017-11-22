#coding=gbk
#by yj,2017.9.5

import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    line = line.split('\t')
    if len(line)<36:
        continue
    req_or_resp = line[0].strip()
    if req_or_resp == 'req':
        print line[34]
