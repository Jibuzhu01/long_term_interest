#coding=gbk
#by yj,2017.9.6

import sys

tag_stay_num = 100
pos_stay_num = 200

def getMinMax(x_list):
    x_max = -1000.0
    x_min = 1000.0
    for x in x_list:
        if x > x_max:
            x_max = x
        if x < x_min:
            x_min = x
    return x_min,x_max

def scalar_data(x,x_min,x_max):
    if x_max == x_min:
        return 1.0
    score = (x-x_min)/(x_max-x_min)
    if score == 0.0:
        score =0.0001
    return round(score,4)

def normalize(info_dict):
    for label in info_dict:
        word_score = info_dict[label]
        score_list = []
        for word in word_score:
            score_list.append(word_score[word])
        x_min,x_max = getMinMax(score_list)
        for word in word_score:
            score = scalar_data(word_score[word],x_min,x_max)
            word_score[word] = score

def output(mid, x_dict):
    output_line = mid + '\t' + 'POS' + '\t'
    for label in x_dict:
        line = ''
        word_score = x_dict[label]#在这一步word_score还是个字典
        word_score = sorted(word_score.iteritems(),key = lambda x:x[1],reverse=True)#在这一步word_score已经变成列表了！
        if label == 'TAG':
            word_score = word_score[0:tag_stay_num]
        else:
            word_score = word_score[0:pos_stay_num]
        for item in word_score:
            line += item[0] + ':' + str(item[1]) + ','
        output_line += line.strip(',') + '\t'
    print output_line.encode('gbk','ignore')


for line in sys.stdin:
    line = line.strip().decode('gbk','ignore')
    if not line:
        continue
    line = line.split('\t')
    mid = line[0].strip()
    feat_list = line[1:]
    feat_dict = {}
    for label_feat in feat_list:
        label = label_feat.split('_')[0]
        feat_dict.setdefault(label,{})
        f_list = label_feat.split('##')
        for feat in f_list:
            s = feat.split(':')
            if len(s)<2:
                continue
            try:
                feat_name = ':'.join(s[0:-1])
                feat_score = float(s[-1])
            except:
                print >> sys.stderr,"feat_name or feat_score wrong", feat_name, feat_score
                continue
            feat_dict[label][feat_name] = feat_score
    normalize(feat_dict)
    output(mid,feat_dict)
