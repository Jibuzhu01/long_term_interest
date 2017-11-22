#/bin/bash
#by yj,2017.9.5

alarm()
{
    msg=""
    for args in $@
    do
        msg="$msg,$args"
    done
    python send_email.py "${msg}" 1
}


day=$1
if [[ -z $day ]];then
    day=`date -d "-6 days" +"%Y%m%d"`
fi

interval=$2
if [[ -z $interval ]];then
    interval=150
fi

#这里收集一些hadoop上的文件夹，在脚本最后删除过期的
HADOOP_DATA_DIR=jzh/online1_output/online1_day/online1_${day}
HADOOP_HOT_INFO_DIR=yuanjun/long_term_interest/article_hot_info
HADOOP_FEATURE_SCORE_DIR=yuanjun/long_term_interest/feature_score
HADOOP_USER_ACTION_INFO_DIR=yuanjun/long_term_interest/user_action_info
HADOOP_MERGE_SCORE_DIR=yuanjun/long_term_interest/merge_score
HADOOP_NORMAL_SCORE_DIR=yuanjun/long_term_interest/normal_score

timestamp=`date +"%Y%m%d%H%M"`
echo "get long term interest program begin at now, there are 4 steps in total"
echo "get long term interest begin at 1st step —— check past info step ${timestamp}"
for i in $( seq 0 80 )
do
    want_day=`date -d "${day} ${i} days ago" +%Y%m%d`
	hadoop fs -test -e ${HADOOP_HOT_INFO_DIR}/${want_day}/article_hot_info_${want_day}.pk
	if [[ $? != 0 ]]; then
	    sh get_article_hot_info/get_article_hot_info.sh $want_day > log/get_article_hot_info_${want_day}_${timestamp}.log 2>&1
    	if [[ $? != 0 ]]; then
            msg="get article hot info fail!"
        	now_time=`date -d" 1 hours ago " +%Y%m%d%H`
	        alarm ${msg} ${want_day} ${now_time} 
        	exit -1
        fi
	fi
	
	hadoop fs -test -e ${HADOOP_FEATURE_SCORE_DIR}/${want_day}/_SUCCESS
    if [[ $? != 0 ]]; then	
        sh get_user_action_info/get_user_action_info.sh $want_day > log/get_user_action_info_${want_day}_${timestamp}.log 2>&1
        if [[ $? != 0 ]]; then
            msg="get user action info fail!"
        	now_time=`date -d" 1 hours ago " +%Y%m%d%H`
	        alarm ${msg} ${want_day} ${now_time} 
	        exit -1
        fi

        sh get_feature_score/get_feature_score.sh $want_day > log/get_feature_score_${want_day}_${timestamp}.log 2>&1
        if [[ $? != 0 ]]; then
            msg="get feature score fail!"
        	now_time=`date -d" 1 hours ago " +%Y%m%d%H`
	        alarm ${msg} ${want_day} ${now_time} 
	        exit -1
        fi
	fi
done

timestamp=`date +"%Y%m%d%H%M"`
echo "get long term interest end at 1st step —— check past info step ${timestamp}"
echo "get long term interest begin at 2nd step —— get merge score step ${timestamp}"
sh get_merge_score/get_merge_score.sh $day $interval > log/get_merge_score_${day}_${timestamp}.log 2>&1
if [[ $? != 0 ]]; then
    msg="get merge score fail!"
	now_time=`date -d" 1 hours ago " +%Y%m%d%H`
	alarm ${msg} ${day} ${now_time} 
	exit -1
fi

timestamp=`date +"%Y%m%d%H%M"`
echo "get long term interest end at 2nd step —— get merge score step ${timestamp}"
echo "get long term interest begin at 3rd step —— get normalization score step ${timestamp}"
sh get_normalization_score/get_normalization_score.sh $day $interval > log/get_normalization_score_${day}_${timestamp}.log 2>&1
if [[ $? != 0 ]]; then
    msg="get normalization score fail!"
	now_time=`date -d" 1 hours ago " +%Y%m%d%H`
	alarm ${msg} ${day} ${now_time} 
	exit -1
fi

timestamp=`date +"%Y%m%d%H%M"`
echo "get long term interest end at 3rd step —— get normalization score step ${timestamp}"
echo "get long term interest begin at 4th step —— update to KV step ${timestamp}"
sh update_to_KV/update_to_KV.sh $day > log/update_to_KV_${day}_${timestamp}.log 2>&1
if [[ $? != 0 ]]; then
    msg="update to KV fail!!"
	now_time=`date -d" 1 hours ago " +%Y%m%d%H`
	alarm ${msg} ${day} ${now_time} 
	exit -1
fi
timestamp=`date +"%Y%m%d%H%M"`
echo "get long term interest end at 4th step —— update to KV step ${timestamp}"

echo "delete step ${timestamp}"
expire_time=150
delete_day=`date -d "${day} ${expire_time} days ago" +%Y%m%d`
hadoop fs -rm -r ${HADOOP_HOT_INFO_DIR}/${delete_day}
hadoop fs -rm -r ${HADOOP_FEATURE_SCORE_DIR}/${delete_day}
hadoop fs -rm -r ${HADOOP_USER_ACTION_INFO_DIR}/${delete_day}
hadoop fs -rm -r ${HADOOP_MERGE_SCORE_DIR}/${delete_day}
hadoop fs -rm -r ${HADOOP_NORMAL_SCORE_DIR}/${delete_day}