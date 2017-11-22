#coding=gbk
#by yj,2017.9.6

import sys
import pickle
import os


day = sys.argv[1]
mid_set = set()
data_dir = sys.argv[2]
for rt,dirs,files in os.walk(data_dir):
    for f in files:
        with open(os.path.join(data_dir,f),'r') as fr:
            for line in fr:
                line = line.strip()
                if not line:
                    continue
                line = line.split('\t')
                mid = line[0]
                mid_set.add(mid)
    

output_dict = 'data/update_mid_' + day + '.pk'
with open(output_dict,'w') as fw:
    pickle.dump(mid_set,fw)

