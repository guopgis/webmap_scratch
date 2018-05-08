# -*- coding: UTF-8 -*-
import os
import sys
import csv
import math
from osgeo import ogr, osr
import requests
import json
import time
import datetime
import exceptions
import Queue
import threading
from tqdm import tqdm

'''
有输入文件中有OD点坐标，直接调用API即可。
'''

exitFlag = 0
base_bus = 'http://restapi.amap.com/v3/direction/transit/integrated'
keys = list()
out_put_data = [] # 最终输出结果
TOTAL_COUNT = 0
CUR_REC = 0

class myThread(threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        #print "Starting " + self.name
        self.process_data(self.name, self.q)
        #print "Exiting " + self.name

    def process_data(self, threadName, q):
        while not exitFlag:
            queueLock.acquire()
            if not workQueue.empty():
                t_req = q.get()

                queueLock.release()
                resp = self.__do_query(t_req)
                #print(t_req[0], t_req[1])
                if resp == -2:
                    time.sleep(2)
                elif resp != -1:
                    data_write = [t_req[0], t_req[1], t_req[2], t_req[3], t_req[4], t_req[5]]
                    data_write.extend(resp)
                    #writer.writerow(data_write)
                    out_put_data.append(data_write)
                    curpos = len(out_put_data)
                    if curpos % 100 == 0:
                        print curpos, TOTAL_COUNT
                else:
                    pass
            else:
                queueLock.release()

    def __do_query(self, param):
        result = {}
        path = '{}?origin={},{}&destination={},{}&city={}&cityd={}&output=json&key={}'.format(
            base_bus, param[2], param[3], param[4], param[5], param[6], param[7], param[8])

        try:
            requests.adapters.DEFAULT_RETRIES = 5
            requests.session().keep_alive = False
            result = requests.get(path, timeout=45).json()
        except Exception as e:
            #print("Exception: {}".format(e))
            return -2
        else:
            r_info = result['info']
            #print(result['infocode'])
            r_count = 0
            if 'count' in result.keys():
                r_count = result['count']
            if r_info == 'OK' and r_count != 0:
                r_route = result['route']
                r_origin = r_route['origin']
                r_destination = r_route['destination']
                r_transits = r_route['transits']
                if len(r_transits) == 0:
                    return -1
                r_sum_dist = int(r_transits[0]['distance'])  # 总距离
                r_sum_duration = int(r_transits[0]['duration'])  # 总耗时
                r_segments = r_transits[0]['segments']
                if len(r_segments) == 0:
                    return -1
                r_trans_count = len(r_segments) - 1  # 公交方案数量
                sum_trans_dist, sum_trans_time = 0, 0  # 公交距离和时间
                walk_to_sta_dist, walk_to_sta_time = 0, 0  # 起点步行到公交站距离和时间
                walk_sta_to_sta_dist, walk_sta_to_sta_time = 0, 0  # 换乘步行距离和时间
                walk_sta_to_destination_dist, walk_sta_to_destination_time = 0, 0  # 公交到目的地步行距离和时间
                k = 0
                for seg in r_segments:
                    walk_dist, walk_time = 0, 0
                    if len(seg['walking']) != 0 and 'distance' in seg['walking'].keys() and 'duration' in seg[
                        'walking'].keys():
                        walk_dist = int(seg['walking']['distance'])
                        walk_time = int(seg['walking']['duration'])
                    if k == 0:
                        walk_to_sta_dist = walk_dist
                        walk_to_sta_time = walk_time
                    elif k == len(r_segments) - 1:
                        walk_sta_to_destination_dist = walk_dist
                        walk_sta_to_destination_time = walk_time
                    else:
                        walk_sta_to_sta_dist += walk_dist
                        walk_sta_to_sta_time += walk_time
                    k += 1

                    r_buslines = seg['bus']['buslines']
                    for busline in r_buslines:
                        dist = int(busline['distance'])
                        time = int(busline['duration'])
                        sum_trans_dist += dist
                        sum_trans_time += time
                        break
                list_r = [r_sum_dist, r_sum_duration, r_trans_count, sum_trans_dist, sum_trans_time,
                          walk_to_sta_dist, walk_to_sta_time, walk_sta_to_sta_dist, walk_sta_to_sta_time,
                          walk_sta_to_destination_dist, walk_sta_to_destination_time]
                return list_r
            else:
                return -1


def read_config_csv(csv_fn):
    csv_data = {}
    with open(csv_fn, 'r') as csvfile:
        rec_reader = csv.reader(csvfile, dialect='excel-tab')
        for row_rec in rec_reader:
            csv_data[row_rec[0]] = row_rec[1]
    return csv_data


def read_od_csv(csv_fn):
    csv_data = list()
    with open(csv_fn, 'r') as csvfile:
        rec_reader = csv.reader(csvfile, dialect='excel')
        first_row = True
        for row_rec in rec_reader:
            if first_row:
                first_row = False
            else:
                csv_data.append(row_rec)
    return csv_data


def read_keys(key_fn):
    keys = list()
    with open(key_fn, 'r') as csvfile:
        rec_reader = csv.reader(csvfile, dialect='excel')
        first_row = True
        for row_rec in rec_reader:
            if len(row_rec) == 0:
                break
            if first_row:
                first_row = False
            else:
                keys.append(row_rec[1])
    return keys


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    threadLock = threading.Lock()
    config_fn = os.path.join(os.getcwd(), 'config_getranstime_od.csv')
    config_dict = read_config_csv(config_fn)

    in_od_csvfn = config_dict['in_od_fn']
    out_city_fn = config_dict['out_time_csvfn']
    in_key_fn = config_dict['in_key_fn']
    city = config_dict['in_city1']
    cityd = config_dict['in_city2']
    
    keys = read_keys(in_key_fn)
    od_dict = read_od_csv(in_od_csvfn)
    TOTAL_COUNT = len(od_dict)
    CUR_REC = 0
    min_keys_count = TOTAL_COUNT / 300000 + 1
    queueLock = threading.Lock()
    workQueue = Queue.Queue()

    # 300个线程名字
    threadList = []
    for i in range(1, 201):
        threadList.append('Thread'+str(i).zfill(3))

    threads = []
    threadID = 1

    # 创建新线程
    for tName in threadList:
        thread = myThread(threadID, tName, workQueue)
        thread.daemon = True
        thread.start()
        threads.append(thread)
        threadID += 1

    # 填充队列
    queueLock.acquire()
    k = 0
    key_count = len(keys)
    for rec in od_dict:
        key_idx = k % key_count
        req = [rec[0], rec[3], rec[1], rec[2], rec[4], rec[5], city, cityd,  keys[key_idx]]
        k += 1
        workQueue.put(req)
    queueLock.release()
    # 等待队列清空
    while not workQueue.empty():
        pass
    # 通知线程是时候退出
    exitFlag = 1
    # 等待所有线程完成
    for t in threads:
        t.join()

    # 输出结果到指定文件
    with open(out_city_fn, 'wb') as out_csvfile:
        writer = csv.writer(out_csvfile)
        newline = ['GRID_FROM', 'GRID_TO', 'LON_FROM', 'LAT_FROM', 'LON_TO', 'LAT_TO',
                   'SUM_DIST', 'SUM_TIME', 'CHANGE_COUNT', 'WALK1_DIST', 'WALK1_TIME',
                   'WALK2_DIST', 'WALK2_TIME', 'WALK3_DIST', 'WALK3_TIME',
                   'TRANS_DIST', 'TRANS_TIME']
        writer.writerow(newline)
        for w_line in out_put_data:
            writer.writerow(w_line)
    # ------------------------
    end_time = datetime.datetime.now()
    # info = 'Finished, cost time %s' % (end_time - start_time).seconds

    print (end_time - start_time).seconds
    print "Exiting Main Thread"