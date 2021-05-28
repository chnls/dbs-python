# dbs
1. `mysql`同步，如`api`中使用`gevent`，建议`pymysql`。`mysqldb`在`mysqlclient`库中，使用需`pip install mysqlclient`
2. 每个方法操作完一个`sql`语句即关闭连接，如`api`中操作数据库语句较多，可修改方法，使用一个连接
3. 关于`pg`, 异步方式推荐`asyncpg`, 同步使用`psycopg2`, 其他各有不支持现象


