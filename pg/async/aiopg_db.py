#! /usr/bin/env python
# -*- coding: utf-8 -*-

# @File    : aiopg_pg.py
# @Date    : 2021-04-23
# @Author  : ls
import aiopg
import psycopg2.extras


class DB(object):
    def __init__(self, host="localhost", port=5432, user="postgres", password="", db_name="", timeout=20, encoding="utf8"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.timeout = timeout
        self.encoding = encoding
        self.dsn = 'dbname={db_name} user={user} password={pwd} host={host} port={port}'.format(db_name=db_name, user=user, pwd=password, host=host, port=port)


class PGDB(DB):

    async def get_conn(self):
        try:
            conn = await aiopg.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                       database=self.db_name, timeout=self.timeout)
            cur = await conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except Exception as e:
            raise e
        return conn, cur

    async def get_conn_by_dsn(self):
        try:
            conn = await aiopg.connect(self.dsn)
            # cur = await conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur = await conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            # cur = await conn.cursor()
        except Exception as e:
            raise e
        return conn, cur

    async def query_one(self, sql, args=None):
        """查询单条"""
        # conn, cur = await self.get_conn()
        conn, cur = await self.get_conn_by_dsn()
        try:
            sql_r = cur.mogrify(sql, args)
            print(sql_r)
            # await cur.execute(sql, args)
            await cur.execute(sql_r)
            data = await cur.fetchone()
            # 不使用cursor_factory=psycopg2.extras.DictCursor
            # fields = [col[0] for col in cur.description]
            # data = dict(zip(fields, data))
        except Exception as e:
            conn.close()
            raise e
        conn.close()
        return dict(data)

    async def query_all(self, sql, args=None):
        """查询所有"""
        # conn, cur = await self.get_conn()
        conn, cur = await self.get_conn_by_dsn()
        try:
            sql_r = cur.mogrify(sql, args)
            # await cur.execute(sql, args)
            await cur.execute(sql_r)
            data = await cur.fetchall()
        except Exception as e:
            conn.close()
            raise e
        conn.close()
        return list(map(dict, data))

    async def query_many(self, sql, args=None, page=1, per_page=10):
        """分页查询"""
        # conn, cur = await self.get_conn()
        conn, cur = await self.get_conn_by_dsn()
        try:
            await cur.execute(sql, args)
            await cur.scroll((page - 1) * per_page)
            data = await cur.fetchmany(per_page)
            size = cur.rowcount
            # n = cur.rownumber  # 当前页最后一个在总数中的索引
        except Exception as e:
            conn.close()
            raise e
        conn.close()
        return list(map(dict, data)), size

    async def insert(self, sql, args=None):
        conn, cur = await self.get_conn_by_dsn()
        try:
            async with cur.begin():
                await cur.execute(sql, args)
                # last_id = cur.lastrowid  # 使用oid创建表
        except Exception as e:
            conn.close()
            raise e
        conn.close()

    # async def insert_many(self, sql, args=None):
    #     """不支持"""
    #     conn, cur = await self.get_conn_by_dsn()
    #     try:
    #         async with cur.begin():
    #             await cur.executemany(sql, args)
    #     except Exception as e:
    #         conn.close()
    #         raise e
    #     conn.close()


class PGDBPool(DB):

    def __init__(self, min_size=10, max_size=100):
        super().__init__()
        self.min_size = min_size
        self.max_size = max_size

    async def get_conn_pool(self):
        try:
            pool = await aiopg.create_pool(host=self.host, port=self.port, user=self.user, password=self.password,
                                           database=self.db_name, command_timeout=self.timeout, minsize=self.min_size,
                                           maxsize=self.max_size, encoding=self.encoding)
        except Exception as e:
            raise e
        return pool

    async def get_conn_pool_by_dsn(self):
        try:
            pool = await aiopg.create_pool(self.dsn)
        except Exception as e:
            raise e
        return pool


if __name__ == '__main__':
    pg = PGDB(password="123456", db_name="medicines")
    import asyncio
    async def r():
        a = await pg.query_one("SELECT * FROM illness WHERE id=%s;", (10 or '1=1',))
        print(a)
        # data, size = await pg.query_many("SELECT * FROM illness WHERE drug_tag=%s;", (10,), page=3, per_page=2)
        # print(data, size)

        # did = await pg.insert("INSERT INTO illness (name, spell, user_id, drug_tag) VALUES(%s, %s, %s, %s) returning id;",  ('脑子有病', 'nzyb', 0, 10))

        # print(did)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(r())
