#coding=gbk
#by yj,2017.9.5

import pickle
import sys
file_name = sys.argv[1]
article_hot_dict = {}

with open(file_name,'r') as fr:
    for line in fr:
        line = line.strip()
        if not line:
            continue
        line = line.split('\t')
        docID = line[0].strip()
        num = int(line[1].strip())
        article_hot_dict[docID] = num

w_file_name = file_name + '.pk'
fw = open(w_file_name,'w')
pickle.dump(article_hot_dict,fw)
