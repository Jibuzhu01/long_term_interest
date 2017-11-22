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

interval=$2
day=$1

HADOOP_FEATURE_SCORE_DIR=yuanjun/long_term_interest/feature_score
HADOOP_MERGE_SCORE_DIR=yuanjun/long_term_interest/merge_score/${day}
WORK_PATH=./get_merge_score

hadoop fs -test -e $HADOOP_MERGE_SCORE_DIR
if [ $? -eq 0 ]; then
    hadoop fs -rm -r $HADOOP_MERGE_SCORE_DIR
fi

inputs=""

for i in $( seq 0 ${interval} )
do
    d=`date -d "${day} ${i} days ago" +%Y%m%d`
	tmp=${HADOOP_FEATURE_SCORE_DIR}/${d}
    hadoop fs -test -e ${tmp}
	if [ $? -eq 0 ]; then
	    inputs=$inputs" -input $tmp"
	else
	    echo "$tmp is not exists!!!"
	fi
done

hadoop fs -test -e $HADOOP_FEATURE_SCORE_DIR
if [ $? -eq 0 ]; then
    hadoop org.apache.hadoop.streaming.HadoopStreaming \
	-D mapred.map.tasks=256 \
	-D mapred.reduce.tasks=256 \
	-D mapred.job.name=wx_app_merge_score \
    -D mapred.task.timeout=3600000 \
    -mapper "cat" \
	-file ${WORK_PATH}/get_merge_score_reducer.py \
	-reducer "python get_merge_score_reducer.py" \
	${inputs} \
	-output ${HADOOP_MERGE_SCORE_DIR} \
	-inputformat KeyValueTextInputFormat
fi

if [[ $? != 0 ]]; then
    msg="get merge score fail at map-reduce step!"
	now_time=`date -d" 1 hours ago" + "%Y%m%d%H"`
	alarm ${msg} ${day} ${now_time} 
	exit -1
fi

timestamp=`date +"%Y%m%d%H%M"`
echo "${timestamp} get merge score success!${day}"