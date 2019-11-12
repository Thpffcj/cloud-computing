# HDFS命令
HDFS="hadoop fs"

# Streaming监听的文件目录，要与Streaming程序中保持一致
streaming_dir="/cloud-computing"

# 清空旧数据
$HDFS -rm "${streaming_dir}"'/tmp/*'>/dev/null 2>&1
$HDFS -rm "${streaming_dir}"'/*'>/dev/null 2>&1
$HDFS -mkdir ${streaming_dir}/tmp

# 生成日志

# 加上时间戳，防止重名
templog="access.`date +'%s'`.log"
# 先将日志放到临时目录，再移动到Streaming监听目录，确保原子性
$HDFS -put test.log ${streaming_dir}/tmp/$templog
$HDFS -mv ${streaming_dir}/tmp/$templog ${streaming_dir}/