#! /usr/bin/env python
# -*- coding: utf-8 -*-

# @File    : pg8000_db.py
# @Date    : 2021-04-29
# @Author  : ls

# 不支持cursor.scroll()
# 不支持字典属性返回

import pg8000
import pg8000.native
import pg8000.dbapi


class DB(object):
    def __init__(self, user="postgres", host="localhost", port=5432, password="", db_name="", timeout=20):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db_name = db_name
        self.timeout = timeout
        self.conn = self.get_connection()
        self.native_conn = self.get_native_connection()

    def get_connection(self):
        try:
            conn = pg8000.connect(self.user, host=self.host, port=self.port, password=self.password, database=self.db_name, timeout=self.timeout)
            # conn = pg8000.dbapi.connect(self.user, host=self.host, port=self.port, password=self.password, database=self.db_name, timeout=self.timeout)
        except Exception as e:
            raise e
        return conn

    def get_native_connection(self):
        try:
            conn = pg8000.native.Connection(self.user, host=self.host, port=self.port, password=self.password, database=self.db_name, timeout=self.timeout)
        except Exception as e:
            raise e
        return conn

    def query_one(self, sql, args=None):
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, args)
                    value = cursor.fetchone()
                    keys = [k[0] for k in cursor.description]
                    data = dict(zip(keys, value))
        except Exception as e:
            raise e
        return data

    def query_many(self, sql, args=None, page=1, count=10):
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, args)
                    # cursor.scroll((page - 1) * count)  # 不支持
                    value = cursor.fetchmany(count)
                    size = cursor.rowcount
                    keys = [k[0] for k in cursor.description]
                    data = []
                    for v in value:
                        data.append(dict(zip(keys, v)))
        except Exception as e:
            raise e
        return data, size

    def query_one_native(self, sql, **args):
        try:
            value = self.native_conn.run(sql, **args)
        except Exception as e:
            raise e
        keys = [k["name"] for k in self.native_conn.columns]
        data = dict(zip(keys, value[0]))
        self.native_conn.close()
        return data


if __name__ == '__main__':
    db = DB(password="123456", db_name="medicines")
    # res = db.query_one("SELECT * FROM illness WHERE id=%s;", (1,))
    # res = db.query_one_native("SELECT * FROM illness WHERE id = :pk;", pk=1)
    # res, s = db.query_many("SELECT * FROM illness WHERE drug_tag=%s LIMIT %s OFFSET %s;", (10, 10, 10))
    res, s = db.query_many("SELECT * FROM illness WHERE drug_tag=%s OFFSET %s;", (10, 0))
    print(res, s)
