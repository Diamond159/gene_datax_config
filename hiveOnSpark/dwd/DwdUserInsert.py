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
		INSERT INTO TABLE yuqing.dwd_hotsearch_userinfo_inc
		PARTITION (operate_date='{operate_date}') 
		VALUES (
			'{_id}', 
			'{name}', 
			{impact_index}, 
			'{verified_info}', 
			{verified}, 
			'{profile_image_url}',
			{register_time},
			'{ip_home}',
			{uid},
			'{description}',
			'{birthday}',
			'{gender}',
			'{source_url}',
			'{save_time}',
			{gather_time},
			'{data_provider}',
			'{site_name}',
			'{site_id}',
			{channel},
			'NULL',
			{insert_time}
		)
		""".format(
			operate_date=row["operate_date"],
			_id=row["_id"],
			name=row["name"],
			impact_index=row["impact_index"],
			verified_info=row["verified_info"],
			verified=row["verified"],
			profile_image_url=row["profile_image_url"],
			register_time=row["register_time"] if row["register_time"] else "NULL",
			ip_home=row["ip_home"] if row["ip_home"] else "NULL",
			uid=row["uid"],
			description=row["description"],
			birthday=row["birthday"] if row["birthday"] else "NULL",
			gender=row["gender"],
			source_url=row["source_url"],
			save_time=row["save_time"],
			gather_time=row["gather_time"],
			data_provider=row["data_provider"],
			site_name=row["site_name"],
			site_id=row["site_id"],
			channel=row["channel"],
			insert_time=row["insert_time"])
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
    .appName("Dwd Userinfo") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.session.timeZone", "UTC") \
    .enableHiveSupport() \
    .getOrCreate()

initialize_logger()

query = """select `_id`,
        name,
        round(followers_count*0.5+friends_count*10+statues_count*0.1) as impact_index,
        verified_info,
        verified,
        profile_image_url,
        register_time,
        ip_home,
        uid,
        description,
        birthday,
        gender,
        source_url,
        save_time,
        gather_time,
        data_provider,
        site_name,
        site_id,
        channel,
        '{0}' as insert_time,
		operate_date
        
from yuqing.ods_weibo_hotsearch_userinfo_inc
where `_id` REGEXP '^[0-9a-fA-F]+$' AND length(`_id`) = 24
and length(name) > 0 and gather_time > '{2}' and gather_time < '{0}'
and operate_date='{1}'""".format(current_timestamp , do_date, last_timestamp)

logger.info("开始执行 SQL 查询")
# 使用SparkSession查询Hive表
try:
    # 执行查询
    result = spark.sql(query)

    # 创建线程池，设置线程数量
    max_workers = 3  # 设置并发线程数量
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