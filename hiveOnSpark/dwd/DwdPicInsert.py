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

# 定义插入目标表的方法
def insert_into_table_batch(rows):
    try:
        for row in rows:
            insert_query = """
    INSERT INTO TABLE yuqing.dwd_hotsearch_textimage_inc 
    PARTITION (operate_date='{operate_date}') 
    VALUES (
        '{_id}',
        '{brief_text}',
        '{content}',
        '{url}',
        '{mid}',
        '{source}',
        '{time}',
        '{publish_time}',
        ARRAY({picture_urls}),
        '{hot_point}',
        '{author_name}',
        '{author_id}',
        '{root_url}',
        '{root_time}',
        '{root_author_name}',
        '{root_content}',
        '{user_url}',
        '{source_url}',
        '{save_time}',
        '{gather_time}',
        '{data_provider}',
        '{themes}',
        '{site_name}',
        '{site_id}',
        {channel},
        '{id}',
        '{root_mid}',
        '{root_author_id}',
        '{root_publish_time}',
        '{insert_time}',
        ARRAY({local_paths}),
		'NULL',
		'NULL',
		'NULL',
		'NULL',
		'False'
		)
		""".format(operate_date=row["operate_date"],_id=row["_id"],brief_text=row["brief_text"],
			content=row["content"],url=row["url"],mid=row["mid"],source=row["source"],time=row["time"],publish_time=row["publish_time"],
			picture_urls=json.dumps(row["picture_urls"])[1:-1],hot_point=row["hot_point"],author_name=row["author_name"],author_id=row["author_id"],root_url=row["root_url"] if row["root_url"] else "NULL",
			root_time=row["root_time"] if row["root_time"] else "NULL",root_author_name=row["root_author_name"] if row["root_author_name"] else "NULL",root_content=row["root_content"] if row["root_content"] else "NULL",user_url=row["user_url"],
			source_url=row["source_url"],save_time=row["save_time"],gather_time=row["gather_time"],data_provider=row["data_provider"],themes=row["themes"],
			site_name=row["site_name"],site_id=row["site_id"],channel=row["channel"],id=row["id"],root_mid=row["root_mid"] if row["root_mid"] else "NULL",root_author_id=row["root_author_id"] if row["root_author_id"] else "NULL",
			root_publish_time=row["root_publish_time"] if row["root_publish_time"] else "NULL",
			insert_time=row["insert_time"],local_paths=json.dumps(row["local_paths"])[1:-1])
            spark.sql(insert_query)
            logger.info("成功插入行：{}".format(row))
    except Exception as e:
        logger.error("插入失败，行：{}".format(row), exc_info=True)

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
    .appName("Dwd Pic") \
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
	split(trim(substring(a.picture_urls, 2, length(a.picture_urls)-2)), ', ') AS picture_urls,
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
	{0} as insert_time,
    collect_list(b.local_path) AS local_paths
FROM
    (
        SELECT *
        FROM yuqing.ods_weibo_hotsearch_detail_inc
        WHERE 
        `_id` REGEXP '^[0-9a-fA-F]+$' AND length(`_id`) = 24
        AND length(content) > 0
        AND content_type = 2
        AND message_type IN (1, 3)
        AND operate_date = '{1}'
		AND gather_time > '{2}'
		AND gather_time < '{0}'
    ) AS a
LEFT JOIN
    yuqing.ods_weibo_hotsearch_download_inc AS b
	ON array_contains(transform(split(trim(substring(a.picture_urls, 2, length(a.picture_urls)-2)), ', '), url -> TRIM(url)), b.source_url)
GROUP BY
    a.`_id`,a.content,a.url,a.mid,a.source,a.`time`,a.publish_time,a.like_count,a.comment_count,a.repost_count,a.author_name,a.author_id,
    a.root_url,a.root_time,a.root_author_name,a.root_content,a.user_url,a.source_url,a.save_time,a.gather_time,a.data_provider,
    a.themes,a.site_name,a.site_id,a.channel,a.id,a.root_mid,a.root_author_id,a.root_publish_time,a.picture_urls,a.operate_date""".format(current_timestamp, do_date, last_timestamp)

logger.info("开始执行 SQL 查询")
# 使用SparkSession查询Hive表
try:
    # 执行查询
    result = spark.sql(query)

    # 创建线程池，设置线程数量
    max_workers = 4  # 设置并发线程数量
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交插入任务至线程池
        futures = []
        for row in result.collect():
            future = executor.submit(insert_into_table_batch, [row])
            futures.append(future)

        # 等待所有任务完成
        wait(futures)

except Exception as e:
    # 发生异常时记录错误信息和完整的异常追踪
    logger.exception("查询失败：")

finally:
    # 关闭资源和线程池
    executor.shutdown()
    spark.stop()

# 打印日志队列中的日志
while not log_queue.empty():
    record = log_queue.get()
    print(record.getMessage())

# 程序执行完成后终止进程
sys.exit(0)