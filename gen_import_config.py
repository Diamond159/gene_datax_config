# coding=utf-8
import datetime
import json
import os
import time
import pymongo
# 版本为 3.12.0 必须相同

ip = "106.53.83.93"
port = "27017"
user = "root"
password = "hdu123456"
auth_db = "admin"
connect_db = "electron_spider"
# HDFS NameNode相关配置，需根据实际情况作出修改
hdfs_nn_host = "agent0"
hdfs_nn_port = "8020"

# 生成配置文件的目标路径，可根据实际情况作出修改
output_path = "/opt/module/datax/job/yuqing"
win_output_path = "D:\\Code\\PythonProject\\config_file"
target_file = "test2"

class gen_import_config(object):
    def __init__(self, user, password, auth_db, port="27017", ip="127.0.0.1", connect_db=None):
        """构造函数"""
        self.client = pymongo.MongoClient("mongodb://" + ip + ":" + port)
        self.database = self.client[connect_db]
        self.database.authenticate(user, password, auth_db)

    # def __del__(self):
    #     """析构函数"""
    #     print("__del__")
    #     self.client.close()

    # 获取mysql连接
    # mongodb://root:hdu123456@43.136.43.226:27017/?authMechanism=SCRAM-SHA-256&authSource=admin&directConnection=true
    def get_connection(self, collection_name):
        return self.database.get_collection(collection_name)

    # 获取表格的元数据  包含列名和数据类型
    def get_mongo_meta(self, collection_name):
        collection = self.get_connection(collection_name)
        pipeline = [
            {"$project": {field: {"$type": "$" + field} for field in collection.find_one()}}
        ]
        # 执行聚合查询
        result = list(collection.aggregate(pipeline))[0]
        return result
        # one = collection.find_one()
        # result = []
        # for key, value in one.items():
        #     key_type = type(value).__name__  # 获取字段的类型名
        #     entry = {key: key_type}  # 构建键值对 {字段名: 类型名}
        #     if entry not in result:  # 添加到结果列表，如果结果列表中不存在该字段
        #         result.append(entry)
        # print(result)

    def get_mongo_columns(self, collection_name):
        meta = self.get_mongo_meta(collection_name)
        return [{"name": key, "type": value} for key, value in meta.items()]

    def get_hive_columns(self, collection_name):
        def type_mapping(type):
            mappings = {
                "objectId": "STRING",
                "string": "STRING",
                "int": "INT",
                "long": "BIGINT",
                "array": "STRING",
                "null": "STRING",
                "bool":"boolean"
            }
            return mappings[type]

        meta = self.get_mongo_meta(collection_name)
        return [{"name": key, "type": type_mapping(value)} for key, value in meta.items()]

    def generate_time(self):
        current_timestamp_ms = int(time.time() * 1000)
        twenty_four_hours_ago_timestamp_ms = current_timestamp_ms - (24 * 60 * 60 * 1000)
        return current_timestamp_ms, twenty_four_hours_ago_timestamp_ms

    def generate_json(self, database, collection, hdfs_nn_host, hdfs_nn_port, win_output_path, target_file):
        job = {
            "job": {
                "setting": {
                    "speed": {
                        "channel": 2
                    }
                },
                "content": [{
                    "reader": {
                        "name": "mongodbreader",
                        "parameter": {
                            "address": ["43.136.43.226:27017"],
                            "userName": "yuqing",
                            "userPassword": "hdu123456",
                            "dbName": database,
                            "collectionName": collection,
                            "column": self.get_mongo_columns(collection),
                            "query": '{\"time_stamp\": {"$gt": ${lastTimeStamp}, "$lt": ${currentTimeStamp}}}'
                        }
                    },
                    "writer": {
                        "name": "hdfswriter",
                        "parameter": {
                            "column": self.get_hive_columns(collection),
                            "compress": "gzip",
                            "defaultFS": "hdfs://" + hdfs_nn_host + ":" + hdfs_nn_port,
                            "fieldDelimiter": "\t",
                            "fileName": collection,
                            "fileType": "text",
                            "path": "${targetdir}",
                            "writeMode": "append"
                        }
                    }
                }]
            }
        }
        if not os.path.exists(win_output_path):
            os.makedirs(win_output_path)
        with open(os.path.join(win_output_path, target_file + ".json"), "w") as f:
            json.dump(job, f)
# python gen_import_config.py -d gmall2022 -t activity_info
# import_data /opt/module/datax/job/yuqing/weiboHotSearchInfo.json /origin_data/yuqing/db/weibo_hot_search_info_inc/$do_date

if __name__ == '__main__':
    client = gen_import_config(user, password, auth_db, port, ip, connect_db)
    cc = "60f51d97be4c12384e63a583/65a5ea87e4b080774a5070eb/testYoutube-1/油管/youtubePostPage"
    tt = "60f51d97be4c12384e63a583/652bc5a613e9f5cca9a64c1f/bigdataTest1015-7/微博/weiboSearch"
    # res = client.get_mongo_columns(cc)
    # res2 = client.get_hive_columns(cc)
    # print(res)
    # print(res2)
    client.generate_json(connect_db, cc, hdfs_nn_host, hdfs_nn_port, win_output_path, target_file)
# 生成的结果是需要进行手动处理的
# https://github.com/alibaba/DataX/blob/master/mongodbreader/doc/mongodbreader.md  注意类型的转换
# https://github.com/alibaba/DataX/blob/master/hdfswriter/doc/hdfswriter.md