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

HADOOP_MERGE_SCORE_DIR=yuanjun/long_term_interest/merge_score/${day}
HADOOP_NORMAL_SCORE_DIR=yuanjun/long_term_interest/normal_score/${day}
WORK_PATH=./get_normalization_score

hadoop fs -test -e $HADOOP_NORMAL_SCORE_DIR
if [ $? -eq 0 ]; then
    hadoop fs -rm -r $HADOOP_NORMAL_SCORE_DIR
fi

hadoop fs -test -e $HADOOP_MERGE_SCORE_DIR
if [ $? -eq 0 ]; then
    hadoop org.apache.hadoop.streaming.HadoopStreaming \
    -D mapred.map.tasks=256 \
    -D mapred.reduce.tasks=256 \
    -D mapred.job.name=wx_app_normal_score \
	-D mapred.output.compress=true \
    -D mapred.output.compression.type=BLOCK \
    -D mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec  \
    -D mapred.task.timeout=3600000 \
	-file ${WORK_PATH}/get_normalization_score_mapper.py \
	-mapper "python get_normalization_score_mapper.py" \
	-input ${HADOOP_MERGE_SCORE_DIR} \
	-output ${HADOOP_NORMAL_SCORE_DIR} \
	-inputformat KeyValueTextInputFormat
fi

if [[ $? != 0 ]]; then
    msg="get normalization score fail at map-reduce step!"
	now_time=`date -d" -1hours" + "%Y%m%d%H"`
	alarm ${msg} ${day} ${now_time} 
	exit -1
fi
	
timestamp=`date +"%Y%m%d%H%M"`
echo "${timestamp} get normalization score success!${day}"	