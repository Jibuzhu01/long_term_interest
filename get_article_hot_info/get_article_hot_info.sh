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
HADOOP_DATA_DIR=jzh/online1_output/online1_day/online1_${day}
HADOOP_HOT_INFO_DIR=yuanjun/long_term_interest/article_hot_info/${day}
WORK_PATH=./get_article_hot_info

hadoop fs -test -e $HADOOP_HOT_INFO_DIR
if [ $? -eq 0 ]; then
    hadoop fs -rmr $HADOOP_HOT_INFO_DIR
fi

timestamp=`date +"%Y%m%d%H%M"`
echo "get article hot info begin at map-reduce step ${timestamp}"
hadoop fs -test -e $HADOOP_DATA_DIR
if [ $? -eq 0 ]; then
    hadoop org.apache.hadoop.streaming.HadoopStreaming \
	-D mapred.map.tasks=64 \
	-D mapred.reduce.tasks=64 \
	-D mapred.job.name=wx_app_get_article_hot_info \
	-D mapred.output.compress=true \
	-D mapred.output.compression.type=BLOCK \
	-D mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec \
	-D mapred.task.timeout=3600000 \
	-file ${WORK_PATH}/get_article_hot_info_mapper.py \
	-mapper "python get_article_hot_info_mapper.py" \
	-file ${WORK_PATH}/get_article_hot_info_reducer.py \
	-reducer "python get_article_hot_info_reducer.py" \
	-input ${HADOOP_DATA_DIR} \
	-output ${HADOOP_HOT_INFO_DIR} \
	-inputformat KeyValueTextInputFormat
fi
timestamp=`date +"%Y%m%d%H%M"`
echo "get article hot info end at map-reduce step ${timestamp}"
if [[ $? != 0 ]]; then
    msg="get article hot info fail at map-reduce step! ${timestamp}"
	alarm ${msg}
	exit -1
fi

rm -rf data/${day}

if [ ! -d "data" ]; then
    mkdir data
fi

hadoop fs -get ${HADOOP_HOT_INFO_DIR} data
if [[ $? != 0 ]]; then
    msg="get article hot info fail at download step!"
	now_time=`date -d" -1hours" + "%Y%m%d%H"`
	alarm ${msg} ${now_time}
	exit -1
fi


rm -rf data/article_hot_info_${day}
lzop -cd data/${day}/*.lzo > data/article_hot_info_${day}
rm -rf data/${day}
if [[ $? != 0 ]]; then
    msg="get article hot info fail at unzip step!"
	timestamp=`date +"%Y%m%d%H%M"`
	alarm ${msg} ${timestamp}
	exit -1
fi
filesize=`ls -l data/article_hot_info_${day} | awk '{ print $5 }'`
minsize=$((10))
if [ $minsize -gt $filesize ]; then
    msg="get article hot info fail, file is too small!"
	timestamp=`date +"%Y%m%d%H%M"`
	alarm ${msg} ${timestamp}
	exit -1
fi

python ${WORK_PATH}/save_to_dict.py data/article_hot_info_${day}
if [[ $? != 0 ]]; then
    msg="get article hot info fail at transfer to dict step!"
	timestamp=`date +"%Y%m%d%H%M"`
	alarm ${msg} ${timestamp}
	exit -1
fi

hadoop fs -rm -r $HADOOP_HOT_INFO_DIR
hadoop fs -mkdir $HADOOP_HOT_INFO_DIR
hadoop fs -put data/article_hot_info_${day}.pk data/article_hot_info_${day} ${HADOOP_HOT_INFO_DIR}
if [[ $? != 0 ]]; then
    msg="get article hot info fail at last step!"
	timestamp=`date +"%Y%m%d%H%M"`
	alarm ${msg} ${timestamp}
	exit -1
fi
echo "get article hot info ${day} success!!"

# save 90 days at local
expire_time=90
delete_day=`date -d "${day} ${expire_time} days ago" +%Y%m%d`
rm -rf data/article_hot_info_${delete_day}
rm -rf data/article_hot_info_${delete_day}.pk