# -*- coding: UTF-8 -*-
# __author__ = 'zhonghua'
# 通过入口url层级获取用户followees链接，得到关联用户的主页url
# 所有的用户url存入redis集合
# 多线程
# 配置文件settings.ini
# 依赖util.py config.py
# 依赖webscraping（需安装）

import os
import time
import cookielib
import urllib
import urllib2
import threading
import util
import config
import redis
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )
from webscraping import common, download, xpath

settings = config.settings
zhihu_url_main = util.Queue(queue_name='zhihu_user_url_main',
                       host=settings['redis_host'],
                       port=int(settings['redis_port']))
zhihu_url_copy = util.Queue(queue_name='zhihu_user_url_copy',
                       host=settings['redis_host'],
                       port=int(settings['redis_port']))

# 定义http参数
D = download.Download()
cookie_jar = cookielib.MozillaCookieJar()
cookie_jar.load('cookies.txt')
opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie_jar))

def get_zhihuuser_from_url(user_url):
    """抓取链接函数
    """
    # 获取followees urls函数
    def get_followees():
        # 构造关注链接，寻找下一层user
        user_url_followees = user_url + '/followees'
        user_url_followers = user_url + '/followers'
        try:
            # 获取关注列表页面
            html_followees = D.get(user_url_followees, delay=0.1, opener=opener, read_cache=False, write_cache=False)
            # 获取关注者列表页面
            html_followers = D.get(user_url_followers, delay=0.1, opener=opener, read_cache=False, write_cache=False)
        except Exception, e:
            print 'Exception in download. {}'.format(str(e))
        else:
            if html_followees and html_followers:
                # xpath解析页面
                # 解析followees
                followees_list = xpath.search(html_followees, '//div[@class="zh-general-list clearfix"]//div[@class="zm-profile-card zm-profile-section-item zg-clear no-hovercard"]//h2[@class="zm-list-content-title"]')
                for i in range(len(followees_list)):
                    # 获取链接写入zhihu_url_main集合
                    zhihu_url_main.put(common.regex_get(followees_list[i], r'href="(.*?)" '))
                # 解析followers
                followers_list = xpath.search(html_followers, '//div[@class="zh-general-list clearfix"]//div[@class="zm-profile-card zm-profile-section-item zg-clear no-hovercard"]//h2[@class="zm-list-content-title"]')
                for i in range(len(followers_list)):
                    # 获取链接写入zhihu_url_main集合
                    zhihu_url_main.put(common.regex_get(followers_list[i], r'href="(.*?)" '))
    # 执行获取函数
    get_followees()
    # 从main集合中抽取url,保证是没有读取过的，即不在copy集合中
    new_user_url = zhihu_url_main.get()
    while zhihu_url_copy.ismember(new_user_url) == 1:
        new_user_url = zhihu_url_main.get()
    # 存入copy集合
    zhihu_url_copy.put(new_user_url)
    #递归
    get_zhihuuser_from_url(new_user_url)
    return

def set_count():
    """统计集合元素函数
    """
    # 计时
    start_time = time.time()
    while True:
        # 统计main中元素数量
        time.sleep(10)
        print 'count in main =', zhihu_url_main.scard(), 'count in copy =', zhihu_url_copy.scard(), 'time_used =', int(time.time() - start_time), 's', time.strftime('%Y-%m-%d %X', time.localtime())

def build_kill_sh():
    """生成scrap_url_zhihu.py的结束进程脚本
    """
    with open('kill_scrap_url_zhihu.sh', 'w') as f:
        f.write('kill -9 %s' % os.getpid())
    os.system('chmod +x kill_scrap_url_zhihu.sh')

def main():
    """主程序
    """
    # 线程队列
    threads = []
    # 入口url
    zhihu_entrance_url = 'http://www.zhihu.com/people/zhang-jia-wei'
    # 利用入口url先抓取一定量user_url，初始化main和copy集合
    threading.Thread(target=get_zhihuuser_from_url, args=[zhihu_entrance_url]).start()
    # 休眠30s
    time.sleep(30)
    # 判断zhihu_url_main集合已经写入数据
    if zhihu_url_main.scard() > 0:
        # 添加其他抓取线程
        # 如添加10个
        for i in range(50):
            url = zhihu_url_main.get()
            threads.append(threading.Thread(target=get_zhihuuser_from_url, args=[url]))
        # 添加计数线程
        threads.append(threading.Thread(target=set_count))
        # 启动线程
        for t in threads:
            t.start()
        # 阻塞主线程
        for t in threads:
            t.join()

if __name__ == '__main__':
    # 生成清除进程脚本
    build_kill_sh()
    # 执行主程序
    main()