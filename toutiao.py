# -*- coding:utf-8 -*-
#设置utf-8编码
import os
import requests
from urllib.parse import urlencode
from hashlib import md5
from multiprocessing.pool import Pool
import time
import pymysql
import random
GROUP_START = 1
GROUP_END = 20

#爬取今日头条列表页
#由于今日头条爬取频繁会封ip  推荐使用 牛魔ip代理或者太阳代理 等自动切换代理ip的软件
def get_page(offset,keyword):
    params = {
        'offset': offset,
        'format': 'json',
        'keyword': keyword,
        'autoload': 'true',
        'count': '20',
        'cur_tab': '1',
        'from': 'news',
    }
    url = 'https://www.toutiao.com/search_content/?' + urlencode(params)
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
    except requests.ConnectionError:
        return None


def get_images(json):
    data = json.get('data')
    if data:
        for item in data:
            # print(item)
            image_list = item.get('image_list')
            title = item.get('title')
            media_name = item.get('media_name');
            datetime = item.get('datetime');
            image01="";
            image02="";
            image03="";
            tag_id =str(item.get('tag_id'));
            # print(image_list)
            if image_list:
                for image in image_list:
                    # len 判断是否为空
                    if len(image_list)==1:
                        image01 = image.get('url');
                    if len(image_list)==2:
                        image01 = image.get('url');
                        image02 = image.get('url');
                    if len(image_list)==3:
                        image01 = image.get('url');
                        image02 = image.get('url');
                        image03 = image.get('url');

            yield {


                'title': title,
                'media_name': media_name,
                'datetime': datetime,
                'image01': image01,
                'image02': image02,
                'image03': image03,
                'tag_id':tag_id
            }


#保存图片到本地
def save_image(item):
    if not os.path.exists(item.get('title')):
        os.mkdir(item.get('title'))
    try:
        local_image_url = item.get('image')
        new_image_url = local_image_url.replace('list','large')
        response = requests.get('http:' + new_image_url)
        if response.status_code == 200:
            file_path = '{0}/{1}.{2}'.format(item.get('title'), md5(response.content).hexdigest(), 'jpg')
            if not os.path.exists(file_path):
                with open(file_path, 'wb')as f:
                    f.write(response.content)
            else:
                print('Already Downloaded', file_path)
    except requests.ConnectionError:
        print('Failed to save image')


def main(offset):
    # 创建连接
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='123456', db='today_news', charset='utf8')
    # 创建游标
    keyword = '娱乐';
    cursor = conn.cursor();
    json = get_page(offset,keyword)
    type =0;
    if json:
        for item in get_images(json):
            # 执行SQL，并返回收影响行数
            #1先通过title在数据库查询存在这条数据如果存在就不加入，不存在添加
            titlestr = item.get('tag_id')
            sql_find = 'SELECT * FROM toutiao WHERE tag_id ='+ titlestr;
            findTitle = cursor.execute(sql_find)
            if findTitle :
                pass
            else:
                #插入数据到头条表
                effect_row = cursor.execute("insert into toutiao(title,media_name,datetime,image01,image02,image03,tag_id,type)values(%s,%s,%s,%s,%s,%s,%s,%s)", (item.get('title'),item.get('media_name'),item.get('datetime'),item.get('image01'),item.get('image02'),item.get('image03'),item.get('tag_id'),type))
                #提交
                conn.commit()
                print(item)
                tag_id =item.get('tag_id');
                sql = "select * from toutiao_detail WHERE tag_id = "+tag_id
                find_row = cursor.execute(sql);
                if find_row:
                    pass
                else:
                    #线程随机休眠1-3
                    time.sleep( random.randint(1,3))
                    #获取详情
                    detail = get_request_detail(tag_id)
                    if detail:
                        for detail_item in getdetail(detail):
                            # 执行SQL，并返回收影响行数
                            # 创建游标
                            cursor = conn.cursor();
                            effect_row_detail = cursor.execute(
                                "insert into toutiao_detail(title,content,detail_source,publish_time,comment_count,url,tag_id)values(%s,%s,%s,%s,%s,%s,%s)",
                                (detail_item.get('title'), detail_item.get('content'), detail_item.get('detail_source'),
                                 str(detail_item.get('publish_time')), detail_item.get('comment_count'),
                                 detail_item.get('url'), tag_id))
                            # 提交，不然无法保存新建或者修改的数据
                            conn.commit()
                            print(detail_item);
                            # 提交，不然无法保存新建或者修改的数据
                    else:
                        print("获取详情失败")
                        #如果获取详情失败就将列表里面的数据删除
                        delete = "DELETE FROM toutiao WHERE tag_id = " + tag_id
                        count = cursor.execute(delete)
                        print(str(count) + "行删除")
                        conn.commit()
        # save_image(item)
    else :
        print("获取列表数据失败")
    # 关闭游标
    cursor.close()
    # 关闭连接
    conn.close()



#请求详情接口
def get_request_detail(keyword):
    url = 'https://m.toutiao.com/i'+keyword+'/info/?i='+keyword
    #设置头部
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml; q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://m.toutiao.com /i"+keyword+"/"} #获取重定向之前地址，不然会出现一只是一个详情的情况  Referer
    try:
        session = requests.Session()
        response = session.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
    except requests.ConnectionError:
        return None

#将数据源放入对象
def getdetail(json):
    data = json.get('data')
    if data:
        # for item in data:
            # print(item)
            content = data.get('content')
            detail_source = data.get('detail_source')
            comment_count = data.get('comment_count')
            publish_time = data.get('publish_time')
            title=data.get("title")
            url=data.get('url')

            #打印
            yield {
                'title': title,
                'content': content,
                'detail_source': detail_source,
                'comment_count': comment_count,
                'publish_time': publish_time,
                'url': url
            }

#运行
if __name__ == '__main__':
    pool = Pool()
    groups = ([x * 20 for x in range(GROUP_START, GROUP_END + 1)])
    pool.map(main, groups)
    pool.close()
    pool.join()