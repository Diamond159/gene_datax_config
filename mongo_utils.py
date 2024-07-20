# -*- encoding: utf-8 -*-
from typing import List, Tuple, Dict
import pymongo
import logging

logging.basicConfig(level=logging.DEBUG)

class MongoDBUtil:
    """
    MongoDB工具类
    """

    def __init__(self, ip="127.0.0.1", db_name=None, port="27017", user="root", password="hdu123456", auth_db="admin"):
        """构造函数"""
        self.client = pymongo.MongoClient(
            f"mongodb://{user}:{password}@{ip}:{port}/{db_name}?authSource={auth_db}",
            serverSelectionTimeoutMS=5000  # 5 秒超时
        )
        self.database = self.client[db_name]

    def __del__(self):
        """析构函数"""
        print("__del__")
        self.client.close()

    def create_database(self, db_name):
        """创建数据库"""
        return self.client.get_database(db_name)

    def drop_database(self, db_name):
        """删除数据库"""
        return self.client.drop_database(db_name)

    def select_database(self, db_name):
        """使用数据库"""
        self.database = self.client[db_name]
        return self.database

    def get_database(self, db_name):
        """使用数据库"""
        return self.client.get_database(db_name)

    def list_database_names(self):
        """获取所有数据库列表"""
        return self.client.list_database_names()

    def create_collection(self, collect_name):
        """创建集合"""
        collect = self.database.get_collection(collect_name)
        if collect is not None:
            print(f"collection {collect_name} already exists")
            return collect
        return self.database.create_collection(collect_name)

    def drop_collection(self, collect_name):
        """删除集合"""
        return self.database.drop_collection(collect_name)

    def get_collection(self, collect_name):
        """获取集合"""
        return self.database.get_collection(collect_name)

    def list_collection_names(self):
        """获取所有集合名称"""
        return self.database.list_collection_names()

    def insert(self, collect_name, documents):
        """插入单条或多条数据"""
        return self.database.get_collection(collect_name).insert_many(documents)

    def insert_one(self, collect_name, document):
        """插入一条数据"""
        return self.database.get_collection(collect_name).insert_one(document)

    def insert_many(self, collect_name, documents):
        """插入多条数据"""
        return self.database.get_collection(collect_name).insert_many(documents)

    def delete_one(self, collect_name, filter, collation=None, hint=None, session=None):
        """删除一条记录"""
        return self.database.get_collection(collect_name).delete_one(filter, collation, hint, session)

    def delete_many(self, collect_name, filter, collation=None, hint=None, session=None):
        """删除多条记录"""
        return self.database.get_collection(collect_name).delete_many(filter, collation, hint, session)

    def find_one_and_delete(self, collect_name, filter, projection=None, sort=None, hint=None, session=None, **kwargs):
        """查询并删除一条记录"""
        return self.database.get_collection(collect_name).find_one_and_delete(filter, projection, sort, hint, session, **kwargs)

    def count_documents(self, collect_name, filter, session=None, **kwargs):
        """查询文档数目"""
        return self.database.get_collection(collect_name).count_documents(filter, session, **kwargs)

    def find_one(self, collect_name, filter=None, *args, **kwargs):
        """查询一条记录"""
        return self.database.get_collection(collect_name).find_one(filter, *args, **kwargs)

    def find(self, collect_name, *args, **kwargs):
        """查询所有记录"""
        return self.database.get_collection(collect_name).find(*args, **kwargs)

    def update(self, collect_name, spec, document, upsert=False, manipulate=False,
               multi=False, check_keys=True, **kwargs):
        """更新记录"""
        return self.database.get_collection(collect_name).update(spec, document, upsert, manipulate, multi, check_keys, **kwargs)

    def update_one(self, collect_name, filter, update, upsert=False, bypass_document_validation=False,
                   collation=None, array_filters=None, hint=None, session=None):
        """更新一条记录"""
        return self.database.get_collection(collect_name).update_one(filter, update, upsert, bypass_document_validation, collation, array_filters, hint, session)

    def update_many(self, collect_name, filter, update, upsert=False, array_filters=None,
                    bypass_document_validation=False, collation=None, hint=None, session=None):
        """更新多条记录"""
        return self.database.get_collection(collect_name).update_many(filter, update, upsert, array_filters, bypass_document_validation, collation, hint, session)

    def find_one_and_update(self, collect_name, filter, update, projection=None, sort=None, upsert=False,
                            return_document=False, array_filters=None, hint=None, session=None, **kwargs):
        """查询并更新一条记录"""
        return self.database.get_collection(collect_name).find_one_and_update(filter, update, projection, sort, upsert, return_document, array_filters, hint, session, **kwargs)

    def list_with_pagination(self, collection_name, condition: Dict,
                             page_no: int = 1, page_size: int = 10,
                             sort_tuples: List[Tuple] = list()) -> List[Dict]:
        """分页查询"""
        items_skipped = (page_no - 1) * page_size
        cursor = self.database.get_collection(collection_name).find(condition).skip(items_skipped)
        if len(sort_tuples) > 0:
            cursor = cursor.sort(sort_tuples).limit(page_size)
        else:
            cursor = cursor.limit(page_size)

        items = [item for item in cursor]
        return items
