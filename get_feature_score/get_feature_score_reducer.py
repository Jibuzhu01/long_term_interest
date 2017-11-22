#coding=gbk
#by yj,2017.9.6

import sys

last_mid = ''
feat_str = ''
error_num = 0
for line in sys.stdin:
    line = line.strip().decode('gbk','ignore')
    if not line:
        continue
    line = line.split('\t')
    if len(line) < 4:
        error_num += 1
        continue
    mid = line[0].strip()
    if mid != last_mid:
        if last_mid:
            output = last_mid + '\t' + str(ts) + '\t' + feat_str
            output = output.strip(',')
            output = output.strip()
            print output.encode('gbk','ignore')
        last_mid = mid
        feat_str = ''
    feat = line[1].strip()
    score = float(line[2].strip())
    ts = int (line[3].strip())
    feat_str += feat + '_' + str(score) + '\t'

if last_mid:
    output = last_mid + '\t' + str(ts) + '\t' + feat_str
    output = output.strip(',')
    output = output.strip()
    print output.encode('gbk','ignore')
print >> sys.stderr,error_num
