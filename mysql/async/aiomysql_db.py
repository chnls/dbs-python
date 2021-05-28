#! /usr/bin/env python
# -*- coding: utf-8 -*-

# @File    : aiomysql_db.py
# @Date    : 2021-04-27
# @Author  : ls

# api中sql语句多，可使用一个连接，将conn连接放每个方法参数，执行完再关闭

import aiomysql


class DBInit(object):

    def __init__(self, host="localhost", user="root", password="", port=3306, db_name=""):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db_name = db_name
        self.charset = "utf8mb4"
        # self.pool = self.get_pool()


class MySQLDB(DBInit):

    async def get_connection(self):
        try:
            conn = await aiomysql.connect(host=self.host, user=self.user, password=self.password, port=self.port,
                                          db=self.db_name, charset=self.charset)
        except Exception as e:
            raise e
        return conn

    async def query_one(self, sql, args=None):
        conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                # print(cur.description)
                data = await cur.fetchone()
        except Exception as e:
            conn.close()
            raise e
        conn.close()
        return data

    async def query_many(self, sql, args=None, page=1, count=10):
        """分页查询  SQL加 SQL_CALC_FOUND_ROWS"""
        conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                await cur.scroll((page - 1) * count)
                data = await cur.fetchmany(count)
                size = cur.rowcount
        except Exception as e:
            conn.close()
            raise e
        conn.close()
        return data, size

    async def query_all(self, sql, args=None):
        """查询所有"""
        conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                data = await cur.fetchall()
        except Exception as e:
            conn.close()
            raise e
        conn.close()
        return data

    async def query_size_data(self, sql, args=None):
        """查询并返回总数 SQL加 SQL_CALC_FOUND_ROWS"""
        conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                data = await cur.fetchall()
                size = cur.rowcount
        except Exception as e:
            conn.close()
            raise e
        conn.close()
        return data, size

    async def insert(self, sql, args=None):
        """插入数据"""
        conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                data = await cur.execute(sql, args)
        except Exception as e:
            await conn.rollback()
            conn.close()
            raise e
        await conn.commit()
        conn.close()
        return data

    async def insert_get_id(self, sql, args=None):
        """插入数据并返回自增id"""
        conn = await self.get_connection()
        try:
            # await conn.begin()  # 好像并不起作用
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                last_id = cur.lastrowid  # id自增
        except Exception as e:
            await conn.rollback()
            conn.close()
            raise e
        await conn.commit()
        conn.close()
        return last_id

    async def insert_many(self, sql, args=None):
        """
        批量插入
        :param sql:
        :param args: tuple list [(),()]
        :return:
        """
        conn = await self.get_connection()
        try:
            # await conn.begin()
            async with conn.cursor(aiomysql.DictCursor) as cur:
                data = await cur.executemany(sql, args)
        except Exception as e:
            await conn.rollback()
            conn.close()
            raise e
        await conn.commit()
        conn.close()
        return data

    async def execute(self, sql, args=None):
        """执行操作。可作为更新和删除"""
        conn = await self.get_connection()
        try:
            # await conn.begin()
            async with conn.cursor(aiomysql.DictCursor) as cur:
                data = await cur.execute(sql, args)
        except Exception as e:
            await conn.rollback()
            conn.close()
            raise e
        await conn.commit()
        conn.close()
        return data

    async def execute_many(self, sql, args=None):
        """
        批量执操作。可作为更新和删除
        :param sql:
        :param args: tuple list [(),()]
        :return:
        """
        conn = await self.get_connection()
        try:
            # await conn.begin()
            async with conn.cursor(aiomysql.DictCursor) as cur:
                data = await cur.executemany(sql, args)
        except Exception as e:
            await conn.rollback()
            conn.close()
            raise e
        await conn.commit()
        conn.close()
        return data

    async def execute_procedure(self, conn, sql):
        """执行存储过程"""
        # conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(sql)
        except Exception as e:
            await conn.rollback()
            raise e
        # conn.close()
        return True

    async def call_procedure(self, conn, procname, args=()):
        """调用写存储过程"""
        # conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                data = await cursor.callproc(procname, args)
        except Exception as e:
            await conn.rollback()
            raise e
        await conn.commit()
        # conn.close()
        return data

    async def fetchone_procedure(self, conn, procname, args=()):
        """调用查询存储过程"""
        # conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.callproc(procname, args)
                data = await cursor.fetchone()
        except Exception as e:
            await conn.rollback()
            raise e
        # conn.close()
        return data

    async def fetchmany_procedure(self, conn, procname, args=(), page=1, count=10):
        """调用查询存储过程"""
        # conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.callproc(procname, args)
                await cursor.scroll((page - 1) * count)
                data = await cursor.fetchmany(count)
                size = cursor.rowcount
        except Exception as e:
            await conn.rollback()
            raise e
        # conn.close()
        return data, size

    async def fetchall_procedure(self, conn, procname, args=()):
        """调用查询存储过程"""
        # conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.callproc(procname, args)
                data = await cursor.fetchall()
                size = cursor.rowcount
        except Exception as e:
            await conn.rollback()
            raise e
        # conn.close()
        return data, size

    async def get_cur(self):
        """
        事务操作，数据异常需要conn.rollback()
        查询需手动fetchone() 或 fetchall() 及rollback()
        """
        conn = await self.get_connection()
        # await conn.begin()
        try:
            cur = await conn.cursor(aiomysql.DictCursor)
        except Exception as e:
            raise e
        return conn, cur

    @staticmethod
    async def close_conn(conn, cur):
        # 关闭游标
        await cur.close()
        # 关闭连接
        await conn.commit()
        # 释放连接
        conn.close()


class MySQLDBPool(DBInit):

    def __init__(self, host="localhost", user="root", password="", port=3306, db_name="", min_conn=10, max_conn=100):
        super(MySQLDBPool, self).__init__(host, user, password, port, db_name)
        self.min_conn = min_conn
        self.max_conn = max_conn

    async def get_connection_pool(self):
        try:
            pool = await aiomysql.create_pool(host=self.host, user=self.user, password=self.password, port=self.port,
                                              db=self.db_name, minsize=self.min_conn, maxsize=self.max_conn,
                                              charset=self.charset)
        except Exception as e:
            raise e
        return pool

    # def get_pool(self):
    #     loop = asyncio.get_event_loop()
    #     task = loop.create_task(self.get_connection_pool())
    #     loop.run_until_complete(task)
    #     conn = task.result()
    #     return conn

    # async def _close(self):
    #     self.pool.close()
    #     await self.pool.wait_closed()

    async def query_one(self, sql, args=None):
        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql, args)
                    # print(cur.description)
                    data = await cur.fetchone()
                except Exception as e:
                    await conn.rollback()
                    raise e
        pool.close()
        await pool.wait_closed()
        return data

    async def query_many(self, sql, args=None, page=1, count=10):
        """分页查询  SQL_CALC_FOUND_ROWS"""
        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql, args)
                    # print(cur.description)
                    await cur.scroll((page - 1) * count)
                    data = await cur.fetchmany(count)
                    size = cur.rowcount
                    # await cur.execute("SELECT FOUND_ROWS() as count;")
                    # count = cur.fetchall()
                except Exception as e:
                    await conn.rollback()
                    raise e
        pool.close()
        await pool.wait_closed()
        # return data, count.result()[0].get("count")
        return data, size

    async def query_all(self, sql, args=None):
        """查询所有"""
        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql, args)
                    data = await cur.fetchall()
                except Exception as e:
                    await conn.rollback()
                    raise e
        pool.close()
        await pool.wait_closed()
        return data

    async def query_size_data(self, sql, args=None):
        """查询并返回总数，sql语句需加 SQL_CALC_FOUND_ROWS"""
        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql, args)
                    data = await cur.fetchall()
                    size = cur.rowcount
                    # await cur.execute("SELECT FOUND_ROWS() as count;")
                    # count = cur.fetchall()
                    # print(count.result()[0].get("count"))
                except Exception as e:
                    await conn.rollback()
                    raise e
        pool.close()
        await pool.wait_closed()
        # return data, count.result()[0].get("count")
        return data, size

    async def insert(self, sql, args=None):
        """插入一条"""
        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    res = await cur.execute(sql, args)
                except Exception as e:
                    await conn.rollback()
                    raise e
                if not res:
                    await conn.rollback()
                    # return
                    raise aiomysql.DatabaseError("SQL Insert nothing")
                await conn.commit()
        pool.close()
        await pool.wait_closed()
        return res

    async def insert_get_id(self, sql, args=None):
        """插入一条[，返回自增id]"""
        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    res = await cur.execute(sql, args)
                    last_id = cur.lastrowid  # id自增
                    # await cur.execute("SELECT LAST_INSERT_ID() as last_insert_id;")  # 自增id
                    # last_id_info = await cur.fetchone()
                except Exception as e:
                    await conn.rollback()
                    raise e
                if not res:
                    await conn.rollback()
                    # return
                    raise aiomysql.DatabaseError("SQL Insert nothing")
                await conn.commit()
        pool.close()
        await pool.wait_closed()
        # return last_id_info.get("last_insert_id")
        return last_id

    async def insert_many(self, sql, args=None):
        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            # await conn.begin()  pool无效
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    data = await cur.executemany(sql, args)
                except Exception as e:
                    await conn.rollback()
                    raise e
                await conn.commit()
                if not data:
                    await conn.rollback()
                    raise
        pool.close()
        await pool.wait_closed()
        return data

    async def execute(self, sql, args=None):
        """数据库更新（无删除操作）"""
        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    data = await cur.execute(sql, args)
                except Exception as e:
                    # logger.error("DB Update Error：{}".format(e))
                    await conn.rollback()
                    raise e
                await conn.commit()
                # if data == 0:
                #     await conn.rollback()
                #     raise
        pool.close()
        await pool.wait_closed()
        return data

    async def execute_procedure(self, pool, sql):
        """执行存储过程"""
        # pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await cursor.execute(sql)
                except Exception as e:
                    await conn.rollback()
                    raise e
        # pool.close()
        # await pool.wait_closed()
        return True

    async def call_procedure(self, pool, procname, args=()):
        """
        调用写存储过程
        返回和参数一致
        """
        # pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    data = await cursor.callproc(procname, args)
                except Exception as e:
                    await conn.rollback()
                    raise e
                await conn.commit()
        # pool.close()
        # await pool.wait_closed()
        return data

    async def fetchone_procedure(self, pool, procname, args=()):
        """调用one查询存储过程"""
        # pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await cursor.callproc(procname, args)
                    data = await cursor.fetchone()
                except Exception as e:
                    await conn.rollback()
                    raise e
                await conn.commit()
        # pool.close()
        # await pool.wait_closed()
        return data

    async def fetchmany_procedure(self, pool, procname, args=(), page=1, count=10):
        """调用many查询存储过程"""
        # pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await cursor.callproc(procname, args)
                    await cursor.scroll((page - 1) * count)
                    data = await cursor.fetchmany(count)
                    size = cursor.rowcount
                except Exception as e:
                    await conn.rollback()
                    raise e
        # pool.close()
        # await pool.wait_closed()
        return data, size

    async def fetchall_procedure(self, pool, procname, args=()):
        """调用all查询存储过程"""
        # pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await cursor.callproc(procname, args)
                    data = await cursor.fetchall()
                    # size = cursor.rowcount
                except Exception as e:
                    await conn.rollback()
                    raise e
        # pool.close()
        # await pool.wait_closed()
        return data

    async def get_cur(self):
        """
        事务操作，数据异常需要conn.rollback()
        查询需手动fetchone() 或 fetchall() 及rollback()
        """
        pool = await self.get_connection_pool()
        conn = await pool.acquire()
        cur = await conn.cursor(aiomysql.DictCursor)
        return pool, conn, cur

    @staticmethod
    async def close_pool(pool):
        pool.close()
        await pool.wait_closed()

    @staticmethod
    async def close_pool_conn(pool, conn, cur):
        # 关闭游标
        await cur.close()
        # 关闭连接
        await conn.commit()
        # 释放连接
        pool.release(conn)
        pool.close()
        await pool.wait_closed()


if __name__ == '__main__':
    import asyncio


    async def t():
        # db = MySQLDB(password="123456", db_name="test")
        db = MySQLDBPool(password="123456", db_name="test")
        # pool = await db.get_connection_pool()
        # pool, conn, cur = await db.get_cur()
        # res = None
        # try:
        #     res = await cur.execute("UPDATE t1 SET name='呵呵' WHERE id=1;")
        # except Exception as e:
        #     print(e)
        #     await conn.rollback()
        #     # logger.error(e)
        #     # return "not ok"
        # await db.close_pool_conn(pool, conn, cur)
        # res = await db.query_one("SELECT * FROM t1;")
        # res = await db.query_many("SELECT * FROM t1;", page=1, count=3)
        # res = await db.query_all("SELECT * FROM t1;")
        # res, s = await db.query_size_data("SELECT * FROM t1;")
        res = await db.insert("INSERT INTO t1 (user_id, name) VALUES (%s, %s);", (1, "ha"))
        # res = await db.insert_get_id("INSERT INTO t1 (user_id, name) VALUES (%s, %s);", (1, "had"))
        # res = await db.insert_many("INSERT INTO t1 (user_id, name) VALUES (%s, %s);", [(1, "hh"), (2, "aaa")])
        # res = await db.execute("UPDATE t1 SET name='呵呵' WHERE id=1;")
        # sql = """CREATE PROCEDURE select_one(pk int)
        #          BEGIN
        #             SELECT * FROM t1 WHERE user_id=pk;
        #          END"""
        # sql = """CREATE PROCEDURE insert_one(user_id int, name varchar(4) )
        #          BEGIN
        #             INSERT INTO t1 (user_id, name) VALUES (user_id, name);
        #          END"""
        # sql = """CREATE PROCEDURE update_one(user_id int, name varchar(4), pk int)
        #                  BEGIN
        #                     UPDATE t1 SET user_id=user_id, name=name WHERE id=pk;
        #                  END"""
        # sql = """CREATE PROCEDURE delete_one(pk int)
        #                  BEGIN
        #                     DELETE FROM t1 WHERE id=pk;
        #                  END"""
        # conn = await db.get_connection()
        # await db.execute_procedure(conn, sql)
        # res,s = await db.fetchall_procedure(conn, "select_one", (2,))
        # res = await db.call_procedure(conn, "insert_one", (4, "vv"))
        # res = await db.call_procedure(conn, "update_one", (4, "v4", 121))
        # res = await db.call_procedure(conn, "delete_one", (2,))
        # conn.close()

        return res


    loop = asyncio.get_event_loop()
    print(loop.run_until_complete(t()))
