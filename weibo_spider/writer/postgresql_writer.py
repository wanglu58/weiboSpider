import copy
import logging
import sys

from .writer import Writer

logger = logging.getLogger('spider.postgresql_writer')


class PostgreSqlWriter(Writer):
    def __init__(self, postgresql_config):
        self.postgresql_config = postgresql_config
        try:
            import psycopg2
            connection = psycopg2.connect(**self.postgresql_config)
            connection.close()
        except Exception:
            logger.warning(u'系统中可能没有安装或正确配置PostgreSql数据库，'
                           u'请先根据系统环境安装或配置PostgreSql，再运行程序')
            sys.exit()

    def _postgresql_create(self, connection, sql):
        """创建PostgreSql数据库或表"""
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()
            cursor.close()
        finally:
            connection.close()

    def _postgresql_create_table(self, sql):
        """创建PostgreSql表"""
        import psycopg2
        connection = psycopg2.connect(**self.postgresql_config)
        self._postgresql_create(connection, sql)

    def _postgresql_insert(self, table, data_list_origin):
        """向PostgreSql表插入或更新数据"""
        import psycopg2
        if len(data_list_origin) > 0:
            # We use this to filter out unset values.
            data_list = [{k: v
                          for k, v in data.items() if v is not None}
                         for data in data_list_origin]

            connection = psycopg2.connect(**self.postgresql_config)
            cursor = connection.cursor()
            for i in data_list:
                update_data = []
                values_data = []
                for key, value in i.items():
                    if isinstance(value, str):
                        value = value.replace("'", '"')
                    values_data.append(value)
                    update_data.append(" {key} = '{value}'".format(
                        key=key, value=value
                    ))
                update = ','.join(update_data)
                keys = ', '.join(i.keys())
                values = tuple(values_data)
                sql_i = """INSERT INTO "{}" ({}) VALUES {}""".format(
                    table, keys, values
                )
                sql_u = """UPDATE "{}" SET{} WHERE id = '{}'""".format(
                    table, update, i.get("id")
                )
                try:
                    cursor.execute(sql_i)
                except Exception:
                    try:
                        connection.rollback()
                        cursor.execute(sql_u)
                    except Exception as e:
                        connection.rollback()
                        logger.exception(e)
                finally:
                    connection.commit()

            cursor.close()
            connection.close()

    def write_weibo(self, weibos):
        """将爬取的微博信息写入PostgreSql数据库"""
        # 创建'weibo'表
        try:
            create_table = """
                    CREATE TABLE IF NOT EXISTS "weibo" (
                    id varchar(10) NOT NULL,
                    user_id varchar(12),
                    content varchar(2000),
                    article_url varchar(200),
                    original_pictures varchar(3000),
                    retweet_pictures varchar(3000),
                    original BOOLEAN NOT NULL DEFAULT TRUE,
                    video_url varchar(300),
                    publish_place varchar(100),
                    publish_time timestamptz NOT NULL,
                    publish_tool varchar(30),
                    up_num INT NOT NULL,
                    retweet_num INT NOT NULL,
                    comment_num INT NOT NULL,
                    PRIMARY KEY (id)
                    )"""
            self._postgresql_create_table(create_table)
            # 在'weibo'表中插入或更新微博数据
            weibo_list = []
            info_list = copy.deepcopy(weibos)
            for weibo in info_list:
                weibo.user_id = self.user.id
                weibo_list.append(weibo.__dict__)
            self._postgresql_insert('weibo', weibo_list)
            logger.info(u'%d条微博写入PostgreSql数据库完毕', len(weibos))
        except Exception as e:
            logger.exception(e)

    def write_user(self, user):
        """将爬取的用户信息写入PostgreSql数据库"""
        try:
            self.user = user

            # 创建'user'表
            create_table = """
                    CREATE TABLE IF NOT EXISTS "user" (
                    id varchar(20) NOT NULL,
                    nickname varchar(30),
                    gender varchar(10),
                    location varchar(200),
                    birthday varchar(40),
                    description varchar(400),
                    verified_reason varchar(140),
                    talent varchar(200),
                    education varchar(200),
                    work varchar(200),
                    weibo_num INT,
                    following INT,
                    followers INT,
                    PRIMARY KEY (id)
                    )"""
            self._postgresql_create_table(create_table)
            self._postgresql_insert('user', [user.__dict__])
            logger.info(u'%s信息写入PostgreSql数据库完毕', user.nickname)
        except Exception as e:
            logger.exception(e)
