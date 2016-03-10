# coding: utf-8
# mysql.py
# 操作MySQL类

import os
import re
import threading
import MySQLdb
import MySQLdb.cursors
from datetime import datetime
from webscraping import common

# 等待多少条SQL语句后执行提交
SQLS_SIZE = 100

# Comparison
COMPARISONS = {'gt': '>',
               'lt': '<',
               'gte': '>=',
               'lte': '<='}

class MySQL:
    """MySQL数据库操作接口
    """
    def retry(func):
        def call(self, *args, **kwargs):
            attempts = 0
            while attempts < 5:
                self.lock.acquire()
                try:
                    return func(self, *args, **kwargs)
                except MySQLdb.Error, e:
                    sql = ''
                    if self.cursor:
                        sql = self.cursor._last_executed                    
                    common.logger.error('Exception in %s: %s, SQL: %s' % (func.__name__, str(e), sql))
                    if 'MySQL server has gone away' in str(e) or 'Lost connection to MySQL server' in str(e):
                        common.logger.error('MySQL server has gone away, will try to re-connect.')
                        # re-connect MySQL
                        self.connect_mysql()
                        attempts += 1
                    else:
                        # No need to retry for other reasons
                        attempts = 5
                finally:
                    self.lock.release()
        return call

    def __init__(self, settings, cursorclass=MySQLdb.cursors.Cursor):
        """初始化 - 连接MySQL
        settings - MySQL的相关配置参数
        """
        # create a thread lock to access MySQL exclusively
        self.lock = threading.Lock()        
        self.cursorclass = cursorclass
        self.settings = settings
        # 连接MySQL数据库
        self.connect_mysql()
        # 待提交的SQL语句数量
        self.sql_num = 0

    def connect_mysql(self):
        """接MySQL数据库
        """
        try:
            self.conn = MySQLdb.connect(host=self.settings['mysql_host'], port=int(self.settings['mysql_port']), user=self.settings['mysql_user'], \
                                        passwd=self.settings['mysql_password'], cursorclass=self.cursorclass, charset='utf8', use_unicode=True)
            self.cursor = self.conn.cursor()
            self.conn.select_db(self.settings['mysql_db'])
        except Exception, e:
            self.conn = None
            self.cursor = None
            common.logger.error('Failed to connect MYSQL, Exception: %s.' % str(e))
            
    def close(self):
        """关闭SQL链接
        """
        self.conn.close()

    def commit(self):
        """提交执行
        """
        # 待提交的SQL语句数量清零
        self.sql_num = 0
        if self.cursor:
            self.conn.commit()

    def execute(self, sql, args):
        """执行SQL语句
        sql - 要执行的SQL语句
        args - 参数列表
        """
        if self.cursor:
            self.cursor.execute(sql, args)
            # 先不提交，等准备SQLS_SIZE条后一次提交
            self.sql_num += 1
            if self.sql_num >= SQLS_SIZE:
                self.commit()

    @retry
    def get_one(self, table, fields_required=['*'], return_dict=False, **kwargs):
        """返回匹配的第一行
        table - 表名
        fields_required - 需要返回的字段列表
        return_dict - 返回字典或元组
        """
        if self.cursor:
            fields = []
            values = []
            for k, v in kwargs.items():
                k, _, c = k.partition('__')
                c = COMPARISONS.get(c, '=')
                fields.append('`%s` %s %%s' % (k, c))
                values.append(v)
            self.cursor.execute('SELECT %s FROM `%s` WHERE %s;' % (', '.join(fields_required), table, ' AND '.join(fields)), tuple(values))
            #print self.cursor._last_executed
            one = self.cursor.fetchone()
            if one:
                if return_dict:
                    return dict(zip([field[0] for field in self.cursor.description], one))
                else:
                    return one
                
    @retry
    def get_all(self, table, fields_required=['*'], return_dict=False, **kwargs):
        """返回匹配的所有行 - 产生一个迭代器
        table - 表名
        fields_required - 需要返回的字段列表
        return_dict - 返回字典或元组
        """
        if self.cursor:
            fields = []
            values = []
            for k, v in kwargs.items():
                k, _, c = k.partition('__')
                c = COMPARISONS.get(c, '=')
                fields.append('`%s` %s %%s' % (k, c))
                values.append(v)
            self.cursor.execute('SELECT %s FROM `%s` WHERE %s;' % (', '.join(fields_required), table, ' AND '.join(fields)), tuple(values))
            #print self.cursor._last_executed
            for one in self.cursor.fetchall():
                if return_dict:
                    yield dict(zip([field[0] for field in self.cursor.description], one))
                else:
                    yield one    

    @retry
    def insert(self, table, bag):
        """Insert one row into table
        table - Table name
        bag - A dict contains all items to insert
        """
        if self.cursor:
            fields = []
            values = []
            update_fileds = []
            update_values = []
            for k, v in bag.items():
                fields.append('`%s`' % k)
                values.append(v)
                update_fileds.append('`%s` = %%s' % k)
                update_values.append(v)
            # 创建时间
            fields.append('`created_at`')
            values.append(datetime.now())
            # 更新时间
            update_fileds.append('`updated_at` = %s')
            update_values.append(datetime.now())
            self.cursor.execute('INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s;' % (table, ', '.join(fields), ', '.join(['%s'] * len(values)), ', '.join(update_fileds)), 
                         tuple(values + update_values))
            self.conn.commit()
            return self.cursor.lastrowid