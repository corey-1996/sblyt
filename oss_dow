# -*- coding: utf-8 -*-
import oss2
from itertools import islice
from Wiki import wiki
import os
import sys
import pickle
import time
if __name__=="__main__":
    stime=time.time()

    base_path=r"C:\Users\weijian.zhang\Desktop\OSS下载\OSS下载程序\OSS下载程序\OSS下载-河南汽车"    
    path=r"\\10.64.64.245\Nebula_V2\客户回收数据\河南车辆"
    wiki_user="weijian.zhang"
    wiki_pwd="Aa123456"


    print("------初始化OSS模块------")
    with open(os.path.join(base_path,"bucket.pkl"),'rb') as f:
        bucket = pickle.load(f)
    print(">>>bucket初始化完成")


    print("------正在加载已装车河南装车的IMEI信息------")
    try:

        my_wiki=wiki()
        my_wiki.login(wiki_user,wiki_pwd)
        IMEIs=my_wiki.getIMEIs()
        print(">>>获取IMEI完成: %s"%IMEIs)
    except AttributeError:
        print(">>>获取IMEI失败，请检查wiki用户名密码和表名")
        sys.exit()

    print("------开始获取OSS视频文件------")
          # oss2.ObjectIteratorr用于遍历文件。
    if os.path.exists(path):
        pass
    else:
        print(">>>存储路径 %s 未建立，正在创建..."%path)
        os.makedirs(path)
        print(">>>%s创建完成"%path)
    
    download_list=[]
    for obj in oss2.ObjectIterator(bucket, prefix = 'nebulav1/'):
        file_url=obj.key
        for IMEI in IMEIs:
            if IMEI in file_url:
                if "DMS" in file_url or "ADAS" in file_url:
                    filename=file_url.split("/")[-1]
                    data_items=filename.split("_")
                    date=data_items[0]
                    if date>'20191011':
                        download_list.append(file_url)

    print("------获取成功，正在分析已下载视频------")
    local_files=[]
    for i in os.walk(path):
        local_files+=i[-1]
    print(local_files)
    to_download_list=[]
    for i in download_list:
        if i.split("/")[-1] not in local_files:
            to_download_list.append(i)
    print("------分析成功，共%s条新纪录------"%len(to_download_list))

    print("-------开始下载OSS视频文件------")
    for x,file_url in enumerate(to_download_list):
        filename=file_url.split("/")[-1]
        print(">>>正在下载%s/%s %s中..."%(x+1,len(download_list),filename))
        data_items=filename.split("_")
        date=data_items[0]
        IMEI=data_items[3]
        cat=data_items[4]
        folderpath=os.path.join(path,date,IMEI,cat)
        filepath=os.path.join(folderpath,filename)
        if os.path.exists(folderpath):
            pass
        else:
            os.makedirs(folderpath)
        bucket.get_object_to_file(file_url,filepath)


    etime=time.time()
    delta=int(etime-stime)
    print(">>>下载完成，用时%s分%s秒"%(round(delta/60,0),delta%60))
