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

dir=`pwd`
day=$1
HADOOP_DATA_DIR=jzh/online1_output/online1_day/online1_${day}
HADOOP_USER_ACTION_INFO_DIR=yuanjun/long_term_interest/user_action_info/${day}
WORK_PATH=./get_user_action_info

hadoop fs -test -e $HADOOP_USER_ACTION_INFO_DIR
if [ $? -eq 0 ];then
    hadoop fs -rm -r $HADOOP_USER_ACTION_INFO_DIR
fi

cp data/article_hot_info_${day}.pk ${WORK_PATH}/article_hot_info_${day}.pk
hadoop fs -test -e $HADOOP_DATA_DIR
if [ $? -eq 0 ];then
    hadoop org.apache.hadoop.streaming.HadoopStreaming \
	-D mapred.map.tasks=256 \
	-D mapred.reduce.tasks=256 \
	-D mapred.job.name=wx_app_get_user_action_info \
	-D mapred.task.timeout=3600000 \
	-file ${WORK_PATH}/article_hot_info_${day}.pk \
	-file ${WORK_PATH}/get_user_action_info_mapper.py \
	-mapper "python get_user_action_info_mapper.py article_hot_info_${day}.pk" \
	-input ${HADOOP_DATA_DIR} \
	-output ${HADOOP_USER_ACTION_INFO_DIR} \
	-inputformat KeyValueTextInputFormat
fi
rm -f ${WORK_PATH}/article_hot_info_${day}.pk

if [[ $? != 0 ]]; then
    msg="get user action info fail at map-reduce step!"
	now_time=`date -d" -1hours" + "%Y%m%d%H"`
	alarm ${msg} ${now_time}
	exit -1
fi
echo "get user action info success!${day}"