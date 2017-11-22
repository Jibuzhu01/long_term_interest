#coding=gbk
#by yj,20170905

import pickle
import sys

#loading dict of article hot
with open(sys.argv[1]) as fr:
    article_hot_dict = pickle.load(fr)

for line in sys.stdin:
    line = line.strip().decode('gbk','ignore')
    if not line:
        continue
    line = line.split('\t')
    req_or_resp = line[0].strip()
    if req_or_resp == 'req' and len(line) >= 35:
        action = line[3].strip()
        read_duration = '0.0'
        #only some of "action" are available
        if action == '3':
            action_type = 'FAVOR'
        elif action == '5':
            action_type = 'SHARE'
        elif action == '6':
            action_type = 'READ'
        elif action == '8':
            action_type = 'QUIT'
            read_duration = line[19].strip()
        else:
            continue
        feature_list = [] #all keywords including "topic/kw" and so on
        mid = line[1].strip()
        topic = line[4].strip()
        if topic:
            topic = 'TOPIC_' + topic
            feature_list.append(topic)
        keywords = line[7].strip()
        keywords_list = keywords.split(',')
        
        if keywords_list:
            for word in keywords_list:
                if not word:
                    continue
                if 'KS_' in word:
                    continue
                if 'TAG_' not in word:
                    word = 'KW_' + word
                feature_list.append(word)
                
        account_id = line[15].strip() #push channel_ID in feature_list
        if account_id:
            account_id = 'ACC_'+account_id
            feature_list.append(account_id)
            
        hot_num=-1 #push doc_ID in feature_list and article_click to output
        docID = line[34].strip()
        if docID and docID in article_hot_dict:
            hot_num=article_hot_dict[docID]
            docID = 'DOCID_'+docID
            feature_list.append(docID)
            
        output = mid + '\t' + action_type + '\t' + ','.join(feature_list) + '\t' + read_duration + '\t' + str(hot_num)
        print output.encode('gbk','ignore')
                
