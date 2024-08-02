# coding=utf-8
from pyspark import SparkContext
from pyspark.sql import SparkSession
import logging
from concurrent.futures import ThreadPoolExecutor, wait
import concurrent.futures
from Queue import Queue
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# 设置日志级别
logging.basicConfig(level=logging.INFO)

def get_hdfs_file_content(file_names):
    sc = SparkContext(appName="ReadHDFSFile")
    contents = []

    for file_name in file_names:
        file_path = "/user/hue/oozie/workspaces/lib/tmp/{}.txt".format(file_name)
        lines = sc.textFile(file_path)
        content = lines.collect()
        if content:
            contents.append(content[0])

    sc.stop()  # 停止 SparkContext，释放资源
    return contents

# 创建日志队列
log_queue = Queue()


def initialize_logger():
    global logger
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# 调用函数并获取文件内容
file_names = ["CURRENT_TIME_STAMP", "LAST_TIME_STAMP", "do_date"]
contents = get_hdfs_file_content(file_names)

current_timestamp = contents[0]
last_timestamp = contents[1]
do_date = contents[2]

spark = SparkSession.builder \
    .appName("Dwd Video") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.session.timeZone", "UTC") \
    .enableHiveSupport() \
    .getOrCreate()

initialize_logger()

query = """SELECT
	a.operate_date as operate_date,
	a.`_id` as `_id`,
    substr(a.content,1, 50) as `brief_text`,
    a.content as content,
    a.url as url,
    a.mid as mid,
    a.source as source,
    a.`time` as `time`,
    a.publish_time as publish_time,
	split(trim(substring(a.video_urls, 2, length(a.video_urls)-2)), ', ') AS video_urls,
	round(a.like_count*0.5+a.comment_count*0.9+a.repost_count*0.8) as hot_point,
    a.author_name as author_name,
    a.author_id as author_id,
    a.root_url as root_url,
    a.root_time as root_time,
    a.root_author_name as root_author_name,
    a.root_content as root_content,
    a.user_url as user_url,
    a.source_url as source_url,
    a.save_time as save_time,
    a.gather_time as gather_time,
    a.data_provider as data_provider,
    a.themes as themes,
    a.site_name as site_name,
    a.site_id as site_id,
    a.channel as channel,
    a.id as id,
    a.root_mid as root_mid,
    a.root_author_id as root_author_id,
    a.root_publish_time as root_publish_time,
	'{0}' as insert_time,
    collect_list(b.local_path) AS local_paths
FROM
    (
        SELECT *
        FROM yuqing.ods_weibo_hotsearch_detail_inc
        WHERE 
        `_id` REGEXP '^[0-9a-fA-F]+$' AND length(`_id`) = 24
        AND length(content) > 0
        AND content_type = 3
        AND message_type IN (1, 3)
        AND operate_date = '{1}'
		AND gather_time > '{2}'
		AND gather_time < '{0}'
    ) AS a
LEFT JOIN
    yuqing.ods_weibo_hotsearch_download_inc AS b
	ON array_contains(transform(split(trim(substring(a.video_urls, 2, length(a.video_urls)-2)), ', '), url -> TRIM(url)), b.source_url)
GROUP BY
    a.`_id`,a.content,a.url,a.mid,a.source,a.`time`,a.publish_time,a.like_count,a.comment_count,a.repost_count,a.author_name,a.author_id,
    a.root_url,a.root_time,a.root_author_name,a.root_content,a.user_url,a.source_url,a.save_time,a.gather_time,a.data_provider,
    a.themes,a.site_name,a.site_id,a.channel,a.id,a.root_mid,a.root_author_id,a.root_publish_time,a.video_urls,a.operate_date""".format(current_timestamp, do_date, last_timestamp)

logger.info("开始执行 SQL 查询")
# 使用SparkSession查询Hive表

try:
    # 执行查询
    result = spark.sql(query)
    print(query)
    collected_result = result.collect()
    # 以行迭代方式输出结果
    for row in collected_result:
        print(row)
except Exception as e:
    # 发生异常时记录错误信息和完整的异常追踪
    print("查询失败：")