#coding=gbk
#by yj,2017.9.6

import sys
import math
import time
import traceback

def output(mid,info_list):
    line = '\t'.join([mid] + info_list)
    print line.encode('gbk','ignore')

def newton_decay(tm):
    newton_parameter = 0.02
    return math.exp(-newton_parameter*tm)

def get_interest_score(score_time_list):
    score = 0.0
    cur_stamp = int(time.time())
    for i in range(len(score_time_list)):
        t_score = score_time_list[i][0]
        t_stamp = score_time_list[i][1]
        interval = (cur_stamp - t_stamp)/(24*3600)
        score += t_score * newton_decay(interval)
    return round(score,4)

def cal_interest(mid,info_dict):
    info_list = []
    for label in info_dict:
        tmp_list = []
        for word in info_dict[label]:
            #对于标签中的每一个word，将其过去到现在全部的天数分数加权平均
            score_time_list = info_dict[label][word]
            score = get_interest_score(score_time_list)
            tmp_list.append((word,score))#tmp_list形式：[(关晓彤,1.1234),(鹿晗,0.9876),(跑男,0.8123)]
        tmp_list.sort(key = lambda x:x[1],reverse = True)
        info_list.append('##'.join((label + '_' + x[0] + ':' +str(x[1]) for x in tmp_list)))
        #每一个label作为列表中的一项，所以总共也没几项
        #info_list形式：[TAG_关晓彤:1.1234##TAG_鹿晗:0.9876##TAG_跑男:0.8123,KW_word1:1.1211##KW_word2:0.9865]
    if len(info_list) > 0:
        output(mid,info_list)
#输出形式："mid+'\t'+TAG_关晓彤:1.1234##TAG_鹿晗:0.9876##TAG_跑男+'\t'+KW_word1:1.1211##KW_word2:0.9865"
       

def merge():
    #本程序输入数据示例：line="1273417+'\t'+time+'\t'+TAG_跑男_1.1234+'\t'+TAG_鹿晗_0.9876"
    pre_mid = ''
    info_dict = {}
    for line in sys.stdin:
        line = line.strip().decode('gbk','ignore')
        if not line:
            continue
        line = line.split('\t')
        mid = line[0].strip()
        try:
            date_stamp = int(line[1].strip())
        except:
            print >> sys.stderr, "date_stamp wrong"
            print >> sys.stderr, line[1]
            continue
        if mid != pre_mid:
            if len(pre_mid) > 0:
                cal_interest(pre_mid,info_dict)
            pre_mid = mid
            info_dict.clear()
#info_dict的形式：{TAG:{鹿晗:[(score1,time1),(score2,time2)]} {跑男:[(score3,time3),(score4,time4)]} KW:{…}}
        for item in line[2:]:
            tups = item.split('_')
            label = tups[0]
            word = '_'.join(tups[1:-1])
            score = min(float(tups[-1]),8.0)
            info_dict.setdefault(label,{})
            info_dict[label].setdefault(word,[])
            info_dict[label][word].append((score,date_stamp))
    cal_interest(mid,info_dict)

if __name__=='__main__':
    merge()
