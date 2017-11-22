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

HADOOP_USER_ACTION_INFO_DIR=yuanjun/long_term_interest/user_action_info/${day}
HADOOP_FEATURE_SCORE_DIR=yuanjun/long_term_interest/feature_score/${day}
WORK_PATH=./get_feature_score

hadoop fs -test -e $HADOOP_FEATURE_SCORE_DIR
if [ $? -eq 0 ]; then
    hadoop fs -rm -r $HADOOP_FEATURE_SCORE_DIR
fi

hadoop fs -test -e $HADOOP_USER_ACTION_INFO_DIR
if [ $? -eq 0 ]; then
    hadoop org.apache.hadoop.streaming.HadoopStreaming \
	-D mapred.map.tasks=256 \
	-D mapred.reduce.tasks=256 \
	-D mapred.job.name=wx_app_get_feature_score \
	-D mapred.task.timeout=3600000 \
	-file ${WORK_PATH}/get_feature_score_mapper.py \
	-mapper "python get_feature_score_mapper.py ${day}" \
	-file ${WORK_PATH}/get_feature_score_reducer.py \
	-reducer "python get_feature_score_reducer.py" \
	-input ${HADOOP_USER_ACTION_INFO_DIR} \
	-output ${HADOOP_FEATURE_SCORE_DIR} \
	-inputformat KeyValueTextInputFormat
fi

if [[ $? != 0 ]]; then
    msg="get feature score fail at map-reduce step!"
	now_time=`date -d" -1hours" + "%Y%m%d%H"`
	alarm ${msg} ${now_time}
	exit -1
fi

timestamp=`date +"%Y%m%d%H%M"`
echo "${timestamp} get feature score success!${day}"