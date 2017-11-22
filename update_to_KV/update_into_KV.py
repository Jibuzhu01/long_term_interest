#coding=gbk

import sys
import pickle
import time
sys.path.append("./gen-py")
sys.path.append("/usr/lib64/python2.6/site-packages/")
from KV import KV
from serialization import *
from user_profile.ttypes import *


profile_expire_time = 86400*30
pos_stay_num = 200
tag_stay_num = 100

conn_profile = KV("110421","long_term_interest")

def update(mid,action,profile_dict):
    if len(profile_dict) == 0:
        return
    profile_str = conn_profile.get(mid)
    user_profile = LongTermUserProfile()
    if profile_str and len(profile_str) > 0:
        try:
            DeserializeThriftMsg(user_profile, profile_str)
        except:
            print >> sys.stderr, "Deserialize %s's profile failed." % mid
        if user_profile.mid != mid:
            print >> sys.stderr, "mid different: %s" % mid
    cur_user_profile = UserProfile()
    cur_user_profile.update_time = int(time.time())
    
    for item in profile_dict:
        feat_name = item.split('_',1)
        if len(feat_name) != 2:
            continue
        prefix = feat_name[0]
        feat = feat_name[1]
        if prefix == 'KW':
            if not cur_user_profile.kw_map:
                cur_user_profile.kw_map = {}
            score = profile_dict[item]
            cur_user_profile.kw_map[feat] = round(score,4)
        elif prefix == 'TAG':
            if not cur_user_profile.tag_map:
                cur_user_profile.tag_map = {}
            score = profile_dict[item]
            cur_user_profile.tag_map[feat] = round(score,4)
        elif prefix == 'TOPIC':
            if not cur_user_profile.topic_map:
                cur_user_profile.topic_map = {}
            score = profile_dict[item]
            cur_user_profile.topic_map[feat] = round(score,4)
        elif prefix == 'ACC':
            if not cur_user_profile.account_map:
                cur_user_profile.account_map = {}
            score = profile_dict[item]
            cur_user_profile.account_map[feat] = round(score,4)
        elif prefix == 'TITLE':
            if not cur_user_profile.title_map:
                cur_user_profile.title_map = {}
            score = profile_dict[item]
            cur_user_profile.title_map[feat] = round(score,4)
        else:
            if not cur_user_profile.other_map:
                cur_user_profile.other_map = {}
            score = profile_dict[item]
            cur_user_profile.other_map[item] = round(score,4)
    
    if cur_user_profile.tag_map:
        cur_user_profile.tag_map = dict(sorted(cur_user_profile.tag_map.iteritems(),key=lambda x:x[1],reverse=True)[0:tag_stay_num])

    if action == 'POS':
        if cur_user_profile.kw_map:
            cur_user_profile.kw_map = dict(sorted(cur_user_profile.kw_map.iteritems(),key=lambda x:x[1],reverse=True)[0:pos_stay_num])
        if cur_user_profile.topic_map:
            cur_user_profile.topic_map = dict(sorted(cur_user_profile.topic_map.iteritems(),key=lambda x:x[1],reverse=True)[0:pos_stay_num])
        if cur_user_profile.account_map:
            cur_user_profile.account_map = dict(sorted(cur_user_profile.account_map.iteritems(),key=lambda x:x[1],reverse=True)[0:pos_stay_num])
        user_profile.pos_info = cur_user_profile
    elif action == 'NEG':
        if cur_user_profile.kw_map:
            cur_user_profile.kw_map = dict(sorted(cur_user_profile.kw_map.iteritems(),key=lambda x:x[1],reverse=True)[0:neg_stay_num])
        if cur_user_profile.topic_map:    
            cur_user_profile.topic_map = dict(sorted(cur_user_profile.topic_map.iteritems(),key=lambda x:x[1],reverse=True)[0:neg_stay_num])
        if cur_user_profile.account_map:
            cur_user_profile.account_map = dict(sorted(cur_user_profile.account_map.iteritems(),key=lambda x:x[1],reverse=True)[0:neg_stay_num])
        user_profile.neg_info = cur_user_profile
    else:
        print >> sys.stderr, action
    user_profile.mid = mid
    user_profile.update_time = int(time.time())
    update_str = SerializeThriftMsg(user_profile)
    conn_profile.set(mid, update_str, expire_time=profile_expire_time)

def run(fr,mid_set):
    i = 0
    for line in fr:
        user_dict = {}
        line = line.strip()
        if not line:
            continue
        line = line.split('\t')
        mid = line[0].strip()
        if mid not in mid_set:
            continue
        action = line[1].strip()
        type_feature_list = line[2:]
        for type_feat in type_feature_list:
            feat_list = type_feat.split(',')
            for feat in feat_list:
                feat = feat.split(':')
                try:
                    feat_name = ':'.join(feat[0:-1])
                    feat_score = float(feat[-1])
                except:
                    print >> sys.stderr,feat
                    continue
                user_dict[feat_name] = feat_score
        i += 1
        update(mid,action,user_dict)
        if i % 1000 == 0:
            print 'process %d records' % i


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print 'Usage: python %s %day %mid_file' % sys.argv[0]
    user_file = sys.argv[1]
    mid_file = sys.argv[2]
    fr = open(user_file,'r')
    mid_set = pickle.load(open(mid_file,'r'))
    start = time.time()
    run(fr,mid_set)
    end = time.time()
    print end - start
    fr.close()
    
