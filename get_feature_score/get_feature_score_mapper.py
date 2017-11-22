#coding=gbk
#by yj,2017.9.6

import sys
import pickle
import datetime
import time
import re

def get_read_num_coef(read_num):
    coef = 1.0
    if read_num > 1000:
        coef = 0.2
    elif read_num < 30:
        coef = 1.2
    else:
        coef = -7.95*10**(-7)*read_num**2 - 0.0002751*read_num + 1.222
    return coef

def get_read_duration_coef(read_duration):
    coef = 1.0
    if read_duration > 2400 or read_duration == -1:
        coef = 0.8
    elif read_duration > 600:
        coef = 1.2
    elif read_duration < 5:
        coef = 0.2
    else:
        coef = read_duration**0.25/3.9
    return coef - 0.2

action_dict = {'READ':0.2,'FAVOR':3.0,'SHARE':5.0,'QUIT':1.0}
feat_list = ['TAG','ACC','TOPIC','KW']
tm = sys.argv[1]
date_stamp=int(time.mktime(time.strptime(tm,'%Y%m%d')))


wrongnum = 0
for line in sys.stdin:
    line = line.strip().decode('gbk','ignore')
    if not line:
        continue
    line = line.split('\t')
    mid = line[0].strip()
    action = line[1].strip()
    if action not in action_dict:
        continue
    feature_str = line[2].strip()
    if not feature_str:
        continue
    try:
        read_num = float(line[-1].strip())
    except:
        print >> sys.stderr, "read_num wrong, wrongnum is %d" %wrongnum
    try:
        read_duration = float(line[-2].strip())
    except:
        print >> sys.stderr, "read_duration wrong, wrongnum is %d" %wrongnum
    feature_list = feature_str.split(',')

    for word in feature_list:
        if not word:
            continue
        score = action_dict[action]

        if word.split('_')[0] not in feat_list:
            continue

        if action == 'READ' or action == 'QUIT':
            score *= get_read_num_coef(read_num)
            if action == 'QUIT':
                score *= get_read_duration_coef(read_duration)
        line = mid + '\t' + word + '\t' +str(score) + '\t' + str(date_stamp)
        print line.encode('gbk','ignore')

    
