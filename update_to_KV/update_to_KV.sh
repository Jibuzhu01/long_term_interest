#/bin/bash
#by yj,2017.9.6

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
HADOOP_TODAY_SCORE_DIR=yuanjun/long_term_interest/feature_score/${day}
HADOOP_NORMAL_SCORE_DIR=yuanjun/long_term_interest/normal_score/${day}
LOCAL_TODAY_SCORE_DIR=data/today_score_${day}
LOCAL_NORMAL_SCORE_DIR=data/normal_score_${day}
dir=`pwd`
WORK_PATH=${dir}/update_to_KV
cd $WORK_PATH

rm -rf $LOCAL_TODAY_SCORE_DIR
hadoop fs -get $HADOOP_TODAY_SCORE_DIR $LOCAL_TODAY_SCORE_DIR
if [[ $? != 0 ]]; then
    msg="update_to_KV fail at download normal_score step!"
	now_time=`date -d" -1hours" + "%Y%m%d%H"`
	alarm ${msg} ${day} ${now_time} 
	exit -1
fi

python get_updated_mid.py $day $LOCAL_TODAY_SCORE_DIR

if [[ $? != 0 ]]; then
    msg="get update mid info fail!"
	now_time=`date -d" -1hours" + "%Y%m%d%H"`
	alarm ${msg} ${day} ${now_time} 
	exit -1
fi

rm -rf $LOCAL_NORMAL_SCORE_DIR
rm -rf data/normal_score_total_${day}
hadoop fs -get $HADOOP_NORMAL_SCORE_DIR $LOCAL_NORMAL_SCORE_DIR
if [[ $? != 0 ]]; then
    msg="update_to_KV fail at download normal_score step!"
	now_time=`date -d" -1hours" + "%Y%m%d%H"`
	alarm ${msg} ${now_time}
	exit -1
fi
lzop -cd ${LOCAL_NORMAL_SCORE_DIR}/*.lzo > data/normal_score_total_${day}
if [[ $? != 0 ]]; then
    msg="update_into_KV fail at unzip step!"
	now_time=`date -d" -1hours" + "%Y%m%d%H"`
	alarm ${msg} ${now_time}
	exit -1
fi
filesize=`ls -l data/normal_score_total_${day} | awk '{ print $5 }'`
minsize=$((10))
if [ $minsize -gt $filesize ]; then
    msg="update_into_KV fail, file is too small!"
	now_time=`date -d" -1hours" + "%Y%m%d%H"`
	alarm ${msg} ${now_time}
	exit -1
fi

python update_into_KV.py data/normal_score_total_${day} data/update_mid_${day}.pk
if [[ $? != 0 ]]; then
    msg="update_into_KV fail!"
	now_time=`date -d" -1hours" + "%Y%m%d%H"`
	alarm ${msg} ${now_time}
	exit -1
fi
rm -rf data/normal_score_total_${day}
rm -rf $LOCAL_TODAY_SCORE_DIR
rm -rf data/today_score_total_${day}
rm -rf $LOCAL_NORMAL_SCORE_DIR