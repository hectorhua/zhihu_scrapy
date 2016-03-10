# coding: utf-8
# config.py
# 鲲鹏分布式采集平台 - 参数配置

import sys
import os
os.chdir(os.path.dirname(os.path.realpath(sys.argv[0])))
import re
import ConfigParser

MAIL_TO = 'qi@site-digger.com,jamp@site-digger.com' # 多个之间用半角分号分隔
MAIL_API_SECURITY_CODE = 'kp2013'
# 同IP下载间隔
DELAY = 10

def read_settings(settings_file='settings.ini'):
    """Read settings
    """
    cfg = ConfigParser.ConfigParser()
    cfg.read(settings_file)
    settings = {}
    # 常规参数
    for option in ['redis_host', 'redis_port', \
                   'mysql_host', 'mysql_port', 'mysql_user', 'mysql_password', 'mysql_db']:
        try:
            settings[option] = cfg.get('setting', option).strip()
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError), e:
            settings[option] = ''
    return settings

def settings2dict(settings_file='settings.ini'):
    """Convert settings into dictionary
    """
    settings = {}
    for line in open(settings_file):
        if not line.startswith('#'):
            m = re.compile(r'([^=]+)\s*=\s*(.+)').search(line)
            if m:
                settings[m.groups()[0].strip()] = m.groups()[1].strip()
    return settings

# 用户可修改的配置参数
settings = read_settings()