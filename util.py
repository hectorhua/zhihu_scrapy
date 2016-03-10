# coding: utf-8
# util.py
# 鲲鹏分布式采集平台 - 基础功能库

import os
import socket
import config
import datetime
import imp
import glob
import time
import hashlib
import functools
import threading
# pip install pyssdb --upgrade
import pyssdb
import redis
import pymongo
from webscraping import common, download

try:
    import cPickle as pickle
except ImportError:
    import pickle

if os.name != 'nt':
    import fcntl
    import struct

    def get_interface_ip(ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s',
                                ifname[:15]))[20:24])

def get_local_ip():
    """ 获取本机IP
    """
    ip = socket.gethostbyname(socket.gethostname())
    if ip.startswith('127.') and os.name != 'nt':
        interfaces = [
            'eth0',
            'eth1',
            'eth2',
            'wlan0',
            'wlan1',
            'wifi0',
            'ath0',
            'ath',
            'ppp0',
            ]
        for ifname in interfaces:
            try:
                ip = get_interface_ip(ifname)
                break
            except IOError:
                pass
    return ip

def send_mail(subject, content, mail_to):
    """Send mail via KP API
    """
    D = download.Download(num_retries=2, read_cache=False, write_cache=False)
    url = 'http://kpapi.sinaapp.com/send-email/?subject=%(subject)s&content=%(content)s&mailto=%(mailto)s&sc=%(sc)s'
    post_data = {}
    post_data['subject'] = subject
    post_data['content'] = content
    post_data['mailto'] = mail_to
    post_data['sc'] = config.MAIL_API_SECURITY_CODE
    D.get(url='http://kpapi.sinaapp.com/send-email/', data=post_data)
    
def get_file_md5(filename):
    """获取文件的MD5值
    """
    md5 = hashlib.md5()
    with open(filename, 'rb') as f: 
        for chunk in iter(lambda: f.read(8192), b''): 
            md5.update(chunk)
    return md5.hexdigest()

class Queue:
    """队列 - 使用SSDB做后端数据存储
    """
    def __init__(self, queue_name='queue', host='127.0.0.1', port=8888):
        self.queue_name = queue_name
        self.redis_client = redis.Redis(host=host, port=port, db = 0)

    def __len__(self):
        """获取当前队列中元素的数量
        """
        # return self.redis_client.qsize(self.queue_name)
        return self.redis_client.scard(self.queue_name)
		

    def serialize(self, value):
        """convert object to a pickled string to save in the db
        """
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    
    def deserialize(self, value):
        """convert pickled string from database back into an object
        """
        return pickle.loads(value)

    def scard(self):
        return self.redis_client.scard(self.queue_name)

    def put(self, msg, hput=False):
        """向队列插入元素
        msg - 要插入队列的数据（支持Object）
        hput - 是否插入队列头部, 默认是插入队列尾
        """
        if not hput:
            return self.redis_client.sadd(self.queue_name, msg)
        else:
            return self.redis_client.sadd(self.queue_name, msg)
            
    def get(self):
        """从队列首部弹出元素
        """
        msg = self.redis_client.srandmember(self.queue_name)
        if msg:
            return msg

    def getall(self):
        """弹出所有元素
        """
        msg = self.redis_client.smember(self.queue_name)
        if msg:
            return msg

    def ismember(self, member):
        """弹出所有元素
        """
        msg = self.redis_client.sismember(self.queue_name, member)
        if msg:
            return msg

    def sdiff(self, queue_diff, queue_copy):
        """返回两个队列元素的差集
        """
        self.redis_client.sdiffstore(queue_diff.queue_name, self.queue_name, queue_copy.queue_name)

    def clear(self):
        """清空队列
        """
        return self.redis_client.delete(self.queue_name)
    
class CACHE:
    """缓存 - 使用SSDB做后端数据存储
    支持Python对象

    cache_name/filename: 
        The name of Hashmap, use this name "filename" to be compatible with PersistentDict in webscraping library
    expires: 
        A timedelta object of how old data can be before expires. By default is set to None to disable. 
    host:
        The SSDB server host
    port:
        The SSDB server port
    """
    def __init__(self, cache_name=None, filename=None, expires=None, host='127.0.0.1', port=8888):
        """initialize a new CACHE
        """
        self.cache_name = filename or cache_name or 'cache'
        self.expires = expires
        self.redis_host = host
        self.redis_port = port
        self.redis_client = pyssdb.Client(host=self.redis_host, port=self.redis_port)

    def __copy__(self):
        """make a copy of current cache settings
        """
        return CACHE(cache_name=self.cache_name, expires=self.expires, host=self.redis_host, port=self.redis_port)

    def __contains__(self, key):
        """check the database to see if a key exists
        """
        return int(self.redis_client.hexists(self.cache_name, self.safe_key(key)))

    def __getitem__(self, key):
        """return the value of the specified key or raise KeyError if not found
        """
        value_meta_update = self.redis_client.hget(self.cache_name, self.safe_key(key))
        if value_meta_update is not None:
            value_meta_update = self.deserialize(value_meta_update)
            if self.is_fresh(value_meta_update.get('updated')):
                return value_meta_update.get('value')
            else:
                raise KeyError("Key `%s' is stale" % key)
        else:
            raise KeyError("Key `%s' does not exist" % key)

    def __delitem__(self, key):
        """remove the specifed value from the database
        """
        self.redis_client.hdel(self.cache_name, self.safe_key(key))

    def __setitem__(self, key, value):
        """set the value of the specified key
        """
        value_meta_update = {'value': value, 'meta': None, 'updated': datetime.datetime.now()}
        self.redis_client.hset(self.cache_name, self.safe_key(key), self.serialize(value_meta_update))
        
    def size(self):
        """get the number of elements in CACHE
        """
        return self.redis_client.hsize(self.cache_name)

    def serialize(self, value):
        """convert object to a pickled string to save in the db
        """
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    
    def deserialize(self, value):
        """convert pickled string from database back into an object
        """
        return pickle.loads(value)
    
    def safe_key(self, key):
        """Make sure the length of key is safe
        """
        if len(key) <= 255:
            return key
        else:
            return hashlib.md5(key).hexdigest()

    def get(self, key, default=None):
        """Get data at key and return default if not defined
        """
        data = default
        if key:
            value_meta_update = self.redis_client.hget(self.cache_name, self.safe_key(key))
            if value_meta_update is not None:
                return self.deserialize(value_meta_update)
        return data

    def meta(self, key, value=None):
        """Get / set meta for this value

        if value is passed then set the meta attribute for this key
        if not then get the existing meta data for this key
        """
        if value is None:
            # want to get meta
            value_meta_update = self.redis_client.hget(self.cache_name, self.safe_key(key))
            if value_meta_update is not None:
                return self.deserialize(value_meta_update).get('meta')
            else:
                raise KeyError("Key `%s' does not exist" % key)
        else:
            # want to set meta
            value_meta_update = self.redis_client.hget(self.cache_name, self.safe_key(key))
            if value_meta_update is not None:
                value_meta_update = self.deserialize(value_meta_update)
                value_meta_update['meta'] = value
                value_meta_update['updated'] = datetime.datetime.now()
                self.redis_client.hset(self.cache_name, self.safe_key(key), self.serialize(value_meta_update))

    def clear(self):
        """Clear all cached data in this collecion
        """
        self.redis_client.hclear(self.cache_name)
        
    def is_fresh(self, t):
        """returns whether this datetime has expired
        """
        return self.expires is None or datetime.datetime.now() - t < self.expires
    
    def close(self):
        """Close all connections
        """
        self.redis_client.close()
        
class Mongo:
    """MongoDB操作接口
    """
    def __init__(self, host, port, user, password, db=None):
        # 数据库连接参数
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        # 创建连接
        self.conn = pymongo.MongoClient(host=self.host, port=int(self.port))
        # 已连接的数据库
        self.dbs = {}
        if db:
            # 默认连接的数据库
            self.db = self[db]
        else:
            self.db = None
        
    def __getitem__(self, db_name):
        """连接指的数据库
        """
        if db_name not in self.dbs:
            self.dbs[db_name] = self.conn[db_name]
            # 验证身份
            if self.user:
                self.dbs[db_name].authenticate(name=self.user, password=self.password)
        return self.dbs[db_name]

def get_all_projects():
    """获取所有采集项目
    """
    projects = []
    for d in glob.glob('projects/*'):
        if '.' not in d:
            projects.append(os.path.basename(d))
    return projects

# 进度队列（所有项目共用）
progress_queue = Queue(queue_name='kpspider_platform_progress',
                       host=config.settings['redis_host'],
                       port=int(config.settings['redis_port']))
class Dispatcher:
    """调度员
    """
    def retry(func):
        MAX_RETRIES_NUM = 1 # 最低重试次数
        def call(self, *args, **kwargs):
            attempts = 0
            while attempts < MAX_RETRIES_NUM:
                try:
                    return func(self, *args, **kwargs)
                except Exception, e:
                    attempts += 1
                    msg = 'Exception in %s: %s' % (func.__name__, str(e))            
                    common.logger.error(msg)
        return call

    def __init__(self, project_name):
        """project_name - 项目名称
        """
        # 项目名称
        self.project_name = project_name
        # 配置参数（全局）
        self.settings = config.settings
        # 每个项目可能会有个性化的设置
        project_settings_file = os.path.join('projects', project_name, 'settings.ini')
        if os.path.exists(project_settings_file):
            project_settings = config.settings2dict(project_settings_file)
            self.settings.update(project_settings)
        # 任务队列
        self.tasks_queue = Queue(queue_name='{0}_tasks'.format(project_name),
                                 host=self.settings['redis_host'],
                                 port=int(self.settings['redis_port']))
        # 输出队列
        self.output_queue = Queue(queue_name='{0}_output'.format(project_name),
                                 host=self.settings['redis_host'], 
                                 port=int(self.settings['redis_port']))
        # 用于记录一些全局变量, 在所有客户端中可读取
        self.vars_shared = CACHE(cache_name='{0}_vars_shared'.format(project_name), 
                                 host=self.settings['redis_host'], 
                                 port=int(self.settings['redis_port']))
        # 标记已经采集到的项目
        self.found = CACHE(cache_name='{0}_found'.format(project_name), 
                           host=self.settings['redis_host'], 
                           port=int(self.settings['redis_port']))
        # 缓存库后面会动态连接
        self.cache_name = None
        self.cache = None
        # MongoDB需要时后面会动态连接
        self.mongo = None
        
    def get_mongo(self):
        """获取MongoDB数据库的连接实例
        """
        if not self.mongo:
            self.mongo = Mongo(host=self.settings['mongo_host'],
                               port=int(self.settings['mongo_port']),
                               db=self.settings['mongo_db'],
                               user=self.settings['mongo_user'],
                               password=self.settings['mongo_password'])
        return self.mongo

    def get_cache(self, extra_name=None):
        """获取缓存对象实例
        extra_name - 缓存对象额外名称
        """
        # 用以HTML缓存
        cache_name = self.settings.get('cache_name') or '{0}_HTML_CACHE'.format(self.project_name)
        if extra_name:
            # 添加额外的名称
            cache_name += '_{}'.format(extra_name)
        if cache_name != self.cache_name:
            # 缓存集合名称发生变化了
            self.cache_name = cache_name
            if self.cache:
                # 关闭之前的缓存
                self.cache.close()
            # 连接新的缓存库
            self.cache = CACHE(cache_name=cache_name, 
                                host=self.settings['redis_host'],
                                port=int(self.settings['redis_port']))
        return self.cache
        
    def get_var(self, name, default=None):
        """读取平台全局变量
        """
        return self.vars_shared.get(name, {}).get('value') or default
    
    def set_var(self, name, value):
        """保存平台全局变量
        """
        self.vars_shared[name] = value

    @retry
    def get_task(self):
        """获取一个任务
        """
        return self.tasks_queue.get()

    @retry
    def add_task(self, new_task, hput=False):
        """加入一个任务
        hput - 是否用头插？
        """
        return self.tasks_queue.put(msg=new_task, hput=hput)

    @retry
    def add_output(self, new_output, hput=False):
        """添加一条输出
        hput - 是否用头插？
        """
        return self.output_queue.put(new_output, hput=hput)
    
    @retry
    def get_output(self):
        """读取一条输出
        """
        return self.output_queue.get()
    
class ModuleObj:
    """模块对象
    """
    def __init__(self, project_name, check_interval=60 * 3):
        """project_name - 项目名称
        check_interval - 每隔多久检测一次模块发生变化，单位: 秒
        """
        self.time_modified = None # 模块文件修改时间
        self.file_md5 = None # 模块文件MD5值
        self.project_name = project_name # 项目名称
        self.module_file = os.path.join('projects', self.project_name, self.project_name + '.py') # 模块文件
        self.module = None # 加载后的模块实例
        # 加载模块
        self.__load()
        # 启动项目处理模块改动监视器   
        self.start_module_monitor(check_interval=check_interval)        
        
    def start_module_monitor(self, check_interval):
        """"检测模块发生变化，自动重新加载
        """
        def monitor():
            while True:
                try:
                    if self.file_md5 != get_file_md5(self.module_file):
                        # 模块文件发生变化了
                        common.logger.info('Module file "{0}" changed. Trying to reload module for project "{1}".'.format(self.module_file, self.project_name))
                        # 重新加载模块
                        self.__load()
                except Exception, e:
                    common.logger.error('Exception in start_module_monitor(): {0}'.format(str(e)))
                finally:
                    # N秒后再检测
                    time.sleep(check_interval)
        threading.Thread(target=monitor).start()
        
    def infor(self):
        """返回模块信息
        """
        return '模块路径:{0}，修改时间:{1}, MD5: {2}'.format(self.module_file, self.time_modified, self.file_md5)

    def __load(self):
        """加载模块
        """
        try:
            self.module = imp.load_source(self.project_name, self.module_file)
        except Exception, e:
            common.logger.error('Exception in ModuleObj.load(): {0}, project "{1}"'.format(str(e), self.project_name))
        else:
            self.file_md5 = get_file_md5(self.module_file) # 模块文件MD5值
            self.time_modified = datetime.datetime.fromtimestamp(os.stat(self.module_file).st_mtime).strftime('%Y-%m-%d %H:%M:%S') # 模块文件修改时间
            if hasattr(self.module, 'load_tasks') and hasattr(self.module, 'process_output') and hasattr(self.module, 'download_extract'):
                return self.module
            else:
                common.logger.error('Invalid project module for project "{0}".'.format(self.project_name))
                
    def func_does_not_exist(self, func_name, *args, **kwargs):
        """模块内函数不存在，默认调用该函数"""
        print 'Method "{}()" does not exist.'.format(func_name)
        
    def __getattr__(self, func_name):
        """调用模块内的函数
        """
        if self.module and hasattr(self.module, func_name):
            return getattr(self.module, func_name)
        else:
            return functools.partial(self.func_does_not_exist, func_name)
        
class ProjectModulesManager:
    """项目处理模块管理器
    动态加载模块，模块修改监控、重新加载
    """
    def __init__(self, check_interval=60 * 3):
        """初始化
        check_interval - 每隔多久检测一次模块发生变化，单位: 秒
        """
        # 存储所有动态加载的模块
        self.modules = {}
        # 每隔多久检测一次模块发生变化
        self.check_interval = check_interval
    
    def get_module_infor(self, project_name):
        """获取模块信息 - 模块路径、载入数据
        """
        if project_name not in self.modules:
            return '处理模块尚未加载'
        else:
            return self.modules[project_name].infor()
  
    def get_module(self, project_name):
        """获取项目处理模块
        """
        if project_name not in self.modules:
            # 加载项目对应的处理模块
            self.modules[project_name] = ModuleObj(project_name=project_name, check_interval=self.check_interval)
        return self.modules[project_name]