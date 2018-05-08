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
import guop.coordinate as Coor
'''
计算两个公交站点之间的公交时间和距离
'''

exitFlag = 0
base_bus = 'http://restapi.amap.com/v3/direction/transit/integrated'

out_put_data = [] # 最终输出结果
TOTAL_COUNT = 0

class myThread(threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        self.process_data(self.name, self.q)

    def process_data(self, threadName, q):
        while not exitFlag:
            queueLock.acquire()
            if not workQueue.empty():
                t_req = q.get()
                queueLock.release()
                resp = self.__do_query(t_req)
                if resp == -2:
                    time.sleep(1)
                    continue
                if resp != -1:
                    data_write = [t_req[0], t_req[1], t_req[2], t_req[3], t_req[4], t_req[5]]
                    data_write.extend(resp)
                    out_put_data.append(data_write)
                threadLock.acquire()
                if len(out_put_data) % 500 == 0:
                    print len(out_put_data), TOTAL_COUNT#, ':', (datetime.datetime.now() - start_time).seconds
                threadLock.release()
            else:
                queueLock.release()

    def __do_query(self, param):
        result = {}
        path = '{}?origin={},{}&destination={},{}&city={}&cityd={}&output=json&key={}'.format(
            base_bus, param[2], param[3], param[4], param[5], param[6], param[7], param[8])

        try:
            requests.adapters.DEFAULT_RETRIES = 5
            requests.session().keep_alive = False
            result = requests.get(path, timeout=20).json()
        except Exception as e:
            #print("Exception: {}".format(e))
            return -2
        else:
            r_info = result['info']
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
                trans_polyline = []
                for seg in r_segments:
                    walk_dist, walk_time = 0, 0
                    if len(seg['walking']) != 0 and 'distance' in seg['walking'].keys() and 'duration' in seg[
                        'walking'].keys():
                        walk_dist = int(seg['walking']['distance'])
                        walk_time = int(seg['walking']['duration'])
                        ###w_steps = seg['walking']['steps']
                        ###for w_step in w_steps:
                            ###w_p = w_step['polyline']
                            ###trans_polyline.append(w_p)
                    if k == 0: #出发点到公交站步行
                        walk_to_sta_dist = walk_dist
                        walk_to_sta_time = walk_time
                    elif k == len(r_segments) - 1: #公交站到目的地步行
                        walk_sta_to_destination_dist = walk_dist
                        walk_sta_to_destination_time = walk_time
                    else:    #公交站换乘步行
                        walk_sta_to_sta_dist += walk_dist
                        walk_sta_to_sta_time += walk_time
                    k += 1

                    r_buslines = seg['bus']['buslines']
                    for busline in r_buslines:
                        dist = int(busline['distance'])
                        time = int(busline['duration'])
                        ###b_p = busline['polyline']
                        ###trans_polyline.append(b_p)
                        sum_trans_dist += dist
                        sum_trans_time += time
                        break
                ###trans_polyline_str = ';'.join(trans_polyline)
                list_r = [r_sum_dist, r_sum_duration, r_trans_count, sum_trans_dist, sum_trans_time,
                          walk_to_sta_dist, walk_to_sta_time, walk_sta_to_sta_dist, walk_sta_to_sta_time,
                          walk_sta_to_destination_dist, walk_sta_to_destination_time]#, trans_polyline_str]
                return list_r
            else:
                return -1


def read_od_csv(csv_fn, from_rec, to_rec):
    csv_data = list()
    with open(csv_fn, 'r') as csvfile:
        rec_reader = csv.reader(csvfile, dialect='excel')
        first_row = True
        k = 0
        for row_rec in rec_reader:
            if first_row:
                first_row = False
            else:
                if from_rec <= k < to_rec:
                    csv_data.append(row_rec)
                k += 1
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
    from_rec = 8700000   # 24586722,每天可600万
    to_rec   = from_rec + 5000000
    TOTAL_COUNT = to_rec - from_rec

    start_time = datetime.datetime.now()
    # print(start_time)
    threadLock = threading.Lock()

    # 输入输出设置
    in_od_csvfn = r'D:\W02_Paper\P005_Pubtrans_Analysis\pubtrans_data\scratch_od_lines.csv'  # 带查询的OD文件
    in_keys_csvfn = r'E:\guop_code\webmap_scratch\guop_keys.csv'

    out_folder = r'D:\W02_Paper\P005_Pubtrans_Analysis\pubtrans_data\out_data'  # 输出文件
    city = '天津'
    cityd = '天津'

    keys = read_keys(in_keys_csvfn)
    od_dict = read_od_csv(in_od_csvfn, from_rec, to_rec)  # 24586722,
    queueLock = threading.Lock()
    workQueue = Queue.Queue()
    # 200个线程名字
    threadList = []
    for i in range(1, 201):
        threadList.append('Thread' + str(i).zfill(3))

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
        req = [rec[0], rec[3], rec[1], rec[2], rec[4], rec[5], city, cityd, keys[key_idx]]
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
    filename = '%s_%sw.csv' % (from_rec/10000, to_rec/10000)
    out_city_fn = os.path.join(out_folder, filename)
    with open(out_city_fn, 'wb') as out_csvfile:
        writer = csv.writer(out_csvfile)
        newline = ['GRID_FROM', 'GRID_TO', 'LON_FROM', 'LAT_FROM', 'LON_TO', 'LAT_TO', 'SUM_DIST', 'SUM_TIME',
                   'CHANGE_COUNT', 'TRANS_DIST', 'TRANS_TIME', 'WALK1_DIST', 'WALK1_TIME', 'WALK2_DIST', 'WALK2_TIME',
                   'WALK3_DIST', 'WALK3_TIME']
        writer.writerow(newline)
        for w_line in out_put_data:
            writer.writerow(w_line)
    # ------------------------
    end_time = datetime.datetime.now()
    # info = 'Finished, cost time %s' % (end_time - start_time).seconds
    print start_time
    print end_time
    print (end_time - start_time).seconds
    print "Exiting Main Thread"