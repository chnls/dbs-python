#! /usr/bin/env python
# -*- coding: utf-8 -*-

# @File    : psycopg2_db.py
# @Date    : 2021-04-27
# @Author  : ls
import psycopg2

import psycopg2.extras


class PGDB(object):
    def __init__(self, host="localhost", port=5432, user="postgres", password="", db_name="", timeout=20):
        self.timeout = timeout
        self.dsn = 'dbname={db_name} user={user} password={pwd} host={host} port={port}'.format(db_name=db_name,
                                                                                                user=user, pwd=password,
                                                                                                host=host, port=port)
        self.conn = self.get_connection()

    def get_connection(self):
        try:
            conn = psycopg2.connect(self.dsn)
        except Exception as e:
            raise e
        return conn

    def query_one(self, sql, args=None):
        """查询单条"""
        # cur = self.conn.cursor()
        try:
            with self.conn:
                with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(sql, args)
                    data = cur.fetchone()
        except Exception as e:
            # self.conn.close()
            raise e
        # cur.close()
        # self.conn.close()
        return dict(data)

    def query_many(self, sql, args=None, page=1, count=10):
        """分页查询"""
        # cur = self.conn.cursor()
        try:
            with self.conn:
                with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(sql, args)
                    cur.scroll((page - 1) * count)
                    data = cur.fetchmany(count)
                    data = list(map(dict, data))
                    size = cur.rowcount
        except Exception as e:
            # self.conn.close()
            raise e
        # cur.close()
        # self.conn.close()
        return data, size

    def query_all(self, sql, args=None):
        """查询所有"""
        try:
            with self.conn:
                with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(sql, args)
                    data = cur.fetchall()
                    data = list(map(dict, data))
        except Exception as e:
            raise e
        return data

    def insert(self, sql, args=None):
        """插入数据"""
        try:
            with self.conn:
                with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(sql, args)
        except Exception as e:
            self.conn.rollback()
            raise e
        self.conn.commit()
        return True

    def insert_get_id(self, sql, args=None):
        """插入数据, 获取自增id"""
        try:
            with self.conn:
                with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(sql, args)
                    last_id = cur.lastrowid  # 需建表使用OID
                    print(last_id)
        except Exception as e:
            self.conn.rollback()
            raise e
        self.conn.commit()
        return True

    def insert_many(self, sql, args):
        """
        批量插入数据
        :param sql:
        :param args: tuple list [(),()]
        :return:
        """
        try:
            with self.conn:
                with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.executemany(sql, args)
        except Exception as e:
            self.conn.rollback()
            raise e
        self.conn.commit()
        return True

    def execute(self, sql, args=None):
        """执行更新删除操作"""
        try:
            with self.conn:
                with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(sql, args)
        except Exception as e:
            self.conn.rollback()
            raise e
        return True

    def execute_many(self, sql, args=None):
        """批量执行更新删除操作"""
        try:
            with self.conn:
                with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    cursor.executemany(sql, args)
        except Exception as e:
            self.conn.rollback()
            raise e
        return True

    # def execute_procedure(self, sql):
    #     """执行存储过程(未测)"""
    #     try:
    #         with self.conn:
    #             with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
    #                 cursor.execute(sql)
    #     except Exception as e:
    #         self.conn.rollback()
    #         raise e
    #     return True
    #
    # def call_procedure(self, procname, args=()):
    #     """
    #     调用写存储过程
    #     返回和参数一致
    #     """
    #     try:
    #         with self.conn:
    #             with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
    #                 data = cursor.callproc(procname, args)
    #     except Exception as e:
    #         self.conn.rollback()
    #         raise e
    #     return data
    #
    # def fetchone_procedure(self, procname, args=()):
    #     """调用查询存储过程"""
    #     try:
    #         with self.conn:
    #             with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
    #                 cursor.callproc(procname, args)
    #                 data = cursor.fetchone()
    #     except Exception as e:
    #         self.conn.rollback()
    #         raise e
    #     return dict(data)
    #
    # def fetchmany_procedure(self, procname, args=(), page=1, count=10):
    #     """调用查询存储过程"""
    #     try:
    #         with self.conn:
    #             with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
    #                 cursor.callproc(procname, args)
    #                 cursor.scroll((page - 1) * count)
    #                 data = cursor.fetchmany(count)
    #                 data = list(map(dict, data))
    #                 size = cursor.rowcount
    #     except Exception as e:
    #         self.conn.rollback()
    #         raise e
    #     return data, size
    #
    # async def fetchall_procedure(self, procname, args=()):
    #     """调用查询存储过程"""
    #     try:
    #         with self.conn:
    #             with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
    #                 cursor.callproc(procname, args)
    #                 data = cursor.fetchall()
    #                 data = list(map(dict, data))
    #                 # size = cursor.rowcount
    #     except Exception as e:
    #         self.conn.rollback()
    #         raise e
    #     return data

    def get_cur(self):
        """
        事务操作
        查询需手动fetch，错误rollback
        """
        try:
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except Exception as e:
            raise e
        return cur

    def close_conn(self, cur):
        self.conn.commit()
        cur.close()
        self.conn.close()


if __name__ == '__main__':
    pg = PGDB(password="123456", db_name="medicines")
    # r = pg.query_one("SELECT * FROM illness WHERE id=%s;", (10,))
    # r, s = pg.query_many("SELECT * FROM illness WHERE drug_tag=%s;", (10,), page=2, count=3)
    # r = pg.query_all("SELECT * FROM illness WHERE drug_tag=%s;", (10,))
    # r = pg.insert("INSERT INTO illness (name, spell, user_id, drug_tag) VALUES(%s, %s, %s, %s) RETURNING id;",  ('脑子有病', 'nzyb', 0, 10))
    # r = pg.insert_get_id("INSERT INTO illness (name, spell, user_id, drug_tag) VALUES(%s, %s, %s, %s);",  ('脑子有病', 'nzyb', 0, 10))
    # r = pg.insert_many("INSERT INTO illness (name, spell, user_id, drug_tag) VALUES(%s, %s, %s, %s);",  [('脑子有病4', 'nzyb', 0, 10), ('脑子有病5', 'nzyb', 0, 10)])
    # r = pg.execute("UPDATE illness SET name=%s, spell=%s, user_id=%s, drug_tag=%s WHERE id=%s;",  ('脑子', 'nzyb', 1, 10, 534))
    # r = pg.execute_many("UPDATE illness SET name=%s, spell=%s, user_id=%s, drug_tag=%s WHERE id=%s;",  [('脑子病4', 'nzyb', 0, 10, 534), ('脑子病5', 'nzyb', 1, 10, 533)])
    # print(r)
