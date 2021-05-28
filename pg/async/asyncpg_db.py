#! /usr/bin/env python
# -*- coding: utf-8 -*-

# @File    : asyncpg_pg.py
# @Date    : 2021-04-23
# @Author  : ls
import asyncpg


class DB(object):
    def __init__(self, host="localhost", port=5432, user="postgres", password="", db_name="", timeout=20):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.timeout = timeout


class PGDB(DB):

    async def get_conn(self):
        try:
            conn = await asyncpg.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                         database=self.db_name, timeout=self.timeout)
        except Exception as e:
            print(e)
            raise e
        return conn

    async def get_conn_by_dsn(self):
        try:
            conn = await asyncpg.connect(
                "postgres://{user}:{password}@{host}:{port}/{database}".format(
                    user=self.user, password=self.password, host=self.host, port=self.port, database=self.db_name)
            )
        except Exception as e:
            print(e)
            raise e
        return conn

    async def query_one(self, sql, *args):
        """返回符合条件的第一条"""
        # conn = await self.get_conn()
        conn = await self.get_conn_by_dsn()
        try:
            row = await conn.fetchrow(sql, *args)
        except Exception as e:
            await conn.close()
            raise e
        await conn.close()
        return dict(row)

    async def query_first_data(self, sql, *args, col=0):
        """返回符合条件的第一条数据索引为默认0的数据"""
        # conn = await self.get_conn()
        conn = await self.get_conn_by_dsn()
        try:
            row = await conn.fetchval(sql, *args, column=col)
        except Exception as e:
            await conn.close()
            raise e
        await conn.close()
        return row

    async def query_all(self, sql, *args):
        """返回符合条件的所有数据"""
        conn = await self.get_conn()
        try:
            row = await conn.fetch(sql, *args)
        except Exception as e:
            await conn.close()
            raise e
        await conn.close()
        return list(map(dict, row))

    async def insert(self, sql, *args):
        """
        插入数据
        """
        conn = await self.get_conn()
        try:
            async with conn.transaction():
                data = await conn.execute(sql, *args)
        except Exception as e:
            await conn.close()
            raise e
        await conn.close()
        data = data.split(" ")[2]
        return data

    async def insert_get_value(self, sql, *args):
        """
        插入数据获取value, SQL语句加 RETURNING value; 返回主键 RETURNING id
        """
        conn = await self.get_conn()
        try:
            row = await conn.fetch(sql, *args)
        except Exception as e:
            await conn.close()
            raise e
        await conn.close()
        return list(map(dict, row))[0]

    async def insert_many(self, sql, args):
        """
        批量查入数据
        :param sql:
        :param args: list
        :return: Bool
        """
        conn = await self.get_conn()
        try:
            async with conn.transaction():
                await conn.executemany(sql, args)
        except Exception as e:
            await conn.close()
            raise e
        await conn.close()
        return True

    async def update(self, sql, *args):
        """更新"""
        conn = await self.get_conn()
        try:
            async with conn.transaction():
                # 修改一条记录, 返回一个字符串
                row = await conn.execute(sql, *args)
                row = row.split(' ')[1]
        except Exception as e:
            await conn.close()
            raise e
        await conn.close()
        return row

    async def update_many(self, sql, args):
        """
        批量更新
        :param sql:
        :param args: tuple list
        :return: Bool
        await conn.executemany("update t1 set name = $1 where id = $2",
                               [("name1", 1), ("name2", 2)])
        """
        conn = await self.get_conn()
        try:
            async with conn.transaction():
                await conn.executemany(sql, args)
        except Exception as e:
            await conn.close()
            raise e
        await conn.close()
        return True

    async def update_return_value(self, sql, *args):
        """更新返回value, SQL 加 RETURNING value（主键）"""
        conn = await self.get_conn()
        try:
            async with conn.transaction():
                row = await conn.fetch(sql, *args)
        except Exception as e:
            await conn.close()
            raise e
        await conn.close()
        return list(map(dict, row))[0]

    async def delete(self, sql, *args):
        """删除"""
        conn = await self.get_conn()
        try:
            async with conn.transaction():
                row = await conn.execute(sql, *args)
                row = row.split(" ")[1]
        except Exception as e:
            await conn.close()
            raise e
        await conn.close()
        return row

    async def delete_many(self, sql, args):
        """
        批量删除
        :param sql:
        :param args: tuple list [(p1,),(p2)]
        :return:
        """
        conn = await self.get_conn()
        try:
            async with conn.transaction():
                await conn.executemany(sql, args)
        except Exception as e:
            await conn.close()
            raise e
        await conn.close()
        return True


class PGDBPool(DB):

    def __init__(self, host="localhost", port=5432, user="postgres", password="", db_name="",
                 min_size=10, max_size=100, max_queries=50000):
        super(PGDBPool, self).__init__(host, port, user, password, db_name)
        self.min_size = min_size
        self.max_size = max_size
        self.max_queries = max_queries  # 最大查询数量, 超过了就换新的连接，默认50000
        # max_inactive_connection_lifetime 最大不活跃时间，默认300，超过则自动关闭

    async def get_conn_pool(self):
        try:
            pool = await asyncpg.create_pool(host=self.host, port=self.port, user=self.user, password=self.password,
                                             database=self.db_name, command_timeout=self.timeout, min_size=self.min_size,
                                             max_size=self.max_size, max_queries=self.max_queries)
        except Exception as e:
            raise e
        return pool

    async def get_conn_pool_by_dsn(self):
        try:
            pool = await asyncpg.create_pool(
                "postgres://{user}:{password}@{host}:{port}/{database}".format(
                    user=self.user, password=self.password, host=self.host, port=self.port, database=self.db_name),
                min_size=self.min_size, max_size=self.max_size, max_queries=self.max_queries)
        except Exception as e:
            raise e
        return pool

    async def query_one(self, sql, *args):
        pool = await self.get_conn_pool()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(sql, *args)
        except Exception as e:
            raise e
        await pool.close()
        return dict(row)

    async def query_first_data(self, sql, *args, col=0):
        """返回符合条件的第一条数据索引为默认0的数据"""
        pool = await self.get_conn_pool()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchval(sql, *args, column=col)
        except Exception as e:
            raise e
        await pool.close()
        return row

    async def query_all(self, sql, *args):
        """返回符合条件的所有数据"""
        pool = await self.get_conn_pool()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetch(sql, *args)
        except Exception as e:
            raise e
        await pool.close()
        return list(map(dict, row))

    async def insert(self, sql, *args):
        """
        插入数据
        """
        pool = await self.get_conn_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    data = await conn.execute(sql, *args)
        except Exception as e:
            raise e
        await pool.close()
        data = data.split(" ")[2]
        return data

    async def insert_get_value(self, sql, *args):
        """
        插入数据获取value, SQL语句加 RETURNING value; 返回主键 RETURNING id
        """
        pool = await self.get_conn_pool()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetch(sql, *args)
        except Exception as e:
            raise e
        await pool.close()
        return list(map(dict, row))[0]

    async def insert_many(self, sql, args):
        """
        批量查入数据
        :param sql:
        :param args: list
        :return: Bool
        """
        pool = await self.get_conn_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(sql, args)
        except Exception as e:
            raise e
        await pool.close()
        return True

    async def update(self, sql, *args):
        """更新"""
        pool = await self.get_conn_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # 修改一条记录, 返回一个字符串
                    row = await conn.execute(sql, *args)
                    row = row.split(' ')[1]
        except Exception as e:
            raise e
        await pool.close()
        return row

    async def update_many(self, sql, args):
        """
        批量更新
        :param sql:
        :param args: tuple list
        :return: Bool
        await conn.executemany("update t1 set name = $1 where id = $2",
                               [("name1", 1), ("name2", 2)])
        """
        pool = await self.get_conn_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(sql, args)
        except Exception as e:
            raise e
        await pool.close()
        return True

    async def update_return_value(self, sql, *args):
        """更新返回value, SQL 加 RETURNING value（主键）"""
        pool = await self.get_conn_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetch(sql, *args)
        except Exception as e:
            raise e
        await pool.close()
        return list(map(dict, row))[0]

    async def delete(self, sql, *args):
        """删除"""
        pool = await self.get_conn_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.execute(sql, *args)
                    row = row.split(" ")[1]
        except Exception as e:
            raise e
        await pool.close()
        return row

    async def delete_many(self, sql, args):
        """
        批量删除
        :param sql:
        :param args: tuple list [(p1,),(p2)]
        :return:
        """
        pool = await self.get_conn_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(sql, args)
        except Exception as e:
            raise e
        await pool.close()
        return True


if __name__ == '__main__':
    # pg = PGDB(password="123456", db_name="medicines")
    pg = PGDBPool(password="123456", db_name="medicines")
    import asyncio
    async def r():
        data = await pg.query_one("SELECT * FROM illness WHERE drug_tag=$1", 10)
        # data = await pg.query_first_data("SELECT * FROM illness WHERE drug_tag=$1", 10)
        # data = await pg.query_all("SELECT * FROM illness WHERE drug_tag=$1", 10)
        # data = await pg.insert("INSERT INTO illness (name, spell, user_id, drug_tag) VALUES ($1, $2, $3, $4);",  '脑子有病', 'nzyb', 0, 10)
        # data = await pg.insert_get_value("INSERT INTO illness (name, spell, user_id, drug_tag) VALUES($1, $2, $3, $4) RETURNING id;",  '脑子有病', 'nzyb', 0, 10)
        # data = await pg.insert_many("INSERT INTO illness (name, spell, user_id, drug_tag) VALUES($1, $2, $3, $4);",  [('aa', 'nzyba', 0, 10),('bb', 'nzyba', 0, 10)])
        # data = await pg.update("update illness set name=$1 where id=$2;", "haha", 513)
        # data = await pg.update_return_value("update illness set name=$1 where id=$2 returning name;", "haha", 513)
        # data = await pg.update_many("update illness set name=$1 where id=$2;", [("haha1", 513),("hehe", 514)])
        # data = await pg.delete("delete from illness where id=$1;", 514,)
        # data = await pg.delete_many("delete from illness where id=$1;", [(503,), (513,)])
        print(data)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(r())
