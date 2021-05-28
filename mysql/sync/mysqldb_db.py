#! /usr/bin/env python
# -*- coding: utf-8 -*-

# @File    : mysqldb_db.py
# @Date    : 2021-04-27
# @Author  : ls

# pip install mysqlclient

import MySQLdb
from MySQLdb.cursors import DictCursor


class DB(object):
    def __init__(self, host="localhost", user="root", password="", port=3306, db_name="", cursorclass=None, autocommit=True):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db_name = db_name
        self.charset = "utf8mb4"
        self.cursorclass = cursorclass  # MySQLdb.cursors.DictCursor
        self.autocommit = autocommit
        self.conn = self.get_connection()

    def get_connection(self):
        try:
            conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.password, database=self.db_name,
                                   charset=self.charset, cursorclass=DictCursor, autocommit=self.autocommit)
        except Exception as e:
            raise e
        return conn

    def query_one(self, sql, args=None):
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, args)
                    data = cursor.fetchone()
        except Exception as e:
            raise e
        return data

    def query_many(self, sql, args=None, page=1, count=10):
        """分页查询，SQL 加 SQL_CALC_FOUND_ROWS"""
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, args)
                    cursor.scroll((page - 1) * count)
                    data = cursor.fetchmany(count)
                    size = cursor.rowcount
        except Exception as e:
            raise e
        return list(data), size

    def query_all(self, sql, args=None):
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, args)
                    data = cursor.fetchall()
        except Exception as e:
            raise e
        return list(data)

    def query_size_data(self, sql, args=None):
        """查询并返回总数 SQL加 SQL_CALC_FOUND_ROWS"""
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, args)
                    data = cursor.fetchall()
                    size = cursor.rowcount
        except Exception as e:
            raise e
        return list(data), size

    def insert(self, sql, args=None):
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    data = cursor.execute(sql, args)
        except Exception as e:
            self.conn.rollback()
            raise e
        return data

    def insert_get_id(self, sql, args=None):
        """插入数据并返回自增id"""
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, args)
                    data = cursor.lastrowid
        except Exception as e:
            self.conn.rollback()
            raise e
        return data

    def insert_many(self, sql, args=None):
        """批量插入"""
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    data = cursor.executemany(sql, args)
        except Exception as e:
            self.conn.rollback()
            raise e
        return data

    async def execute(self, sql, args=None):
        """执行更新删除操作"""
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    data = cursor.execute(sql, args)
        except Exception as e:
            self.conn.rollback()
            raise e
        return data

    def execute_many(self, sql, args=None):
        """批量执行更新删除操作"""
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    data = cursor.executemany(sql, args)
        except Exception as e:
            self.conn.rollback()
            raise e
        return data

    def execute_procedure(self, sql):
        """执行存储过程"""
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql)
        except Exception as e:
            self.conn.rollback()
            raise e
        return True

    def call_procedure(self, procname, args=()):
        """
        调用写存储过程
        返回和参数一致
        """
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    data = cursor.callproc(procname, args)
        except Exception as e:
            self.conn.rollback()
            raise e
        return data

    def fetchone_procedure(self, procname, args=()):
        """调用查询存储过程"""
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.callproc(procname, args)
                    data = cursor.fetchone()
        except Exception as e:
            self.conn.rollback()
            raise e
        return data

    def fetchmany_procedure(self, procname, args=(), page=1, count=10):
        """调用查询存储过程"""
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.callproc(procname, args)
                    cursor.scroll((page - 1) * count)
                    data = cursor.fetchmany(count)
                    size = cursor.rowcount
        except Exception as e:
            self.conn.rollback()
            raise e
        return data, size

    async def fetchall_procedure(self, procname, args=()):
        """调用查询存储过程"""
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.callproc(procname, args)
                    data = cursor.fetchall()
                    # size = cursor.rowcount
        except Exception as e:
            self.conn.rollback()
            raise e
        return data

    def get_cur(self):
        """
        事务操作
        查询需手动fetch，错误rollback
        """
        try:
            cur = self.conn.cursor()
        except Exception as e:
            raise e
        return cur

    def close_conn(self, cur):
        self.conn.commit()
        cur.close()
        self.conn.close()


if __name__ == '__main__':
    db = DB(password="123456", db_name="test")
    # r = db.query_one("SELECT * FROM t1 WHERE id=%s;", (1,))
    # r, s = db.query_many("SELECT SQL_CALC_FOUND_ROWS * FROM t1;", page=2, count=3)
    # r = db.query_all("SELECT * FROM t1 WHERE user_id=%s;", (1,))
    # r, s = db.query_size_data("SELECT SQL_CALC_FOUND_ROWS * FROM t1 WHERE user_id=%s;", (1,))
    # r = db.insert("INSERT INTO t1 (user_id, name) VALUES (%s, %s);", (1, "yaay"))
    # r = db.insert_get_id("INSERT INTO t1 (user_id, name) VALUES (%s, %s);", (1, "ya"))
    # r = db.insert_many("INSERT INTO t1 (user_id, name) VALUES (%s, %s);", [(2, "yaf"), (2, "ya")])
    # r = db.execute_many("UPDATE t1 SET name=%s WHERE id=%s;", [("haha", 101), ("hehe", 102)])
    cur = None
    try:
        cur = db.get_cur()
    except Exception as e:
        print(e)
        raise
    r = cur.executemany("INSERT INTO t1 (user_id, name) VALUES (%s, %s);", [(2, "yaf1"), (2, "yea")])
    db.close_conn(cur)
    print(r)
    # print(r, s)
