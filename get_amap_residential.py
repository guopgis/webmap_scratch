# -*- coding: UTF-8 -*-
import requests
import random
import urllib
import guop.coordinate as CoorTrans
import guop.shpfile_io as ShpIO
import os
import sys, csv
import math
import time
from osgeo import ogr, osr, gdal

base_api = 'http://restapi.amap.com/v3/place/polygon'
req_type = '120300|120301|120302|120303|120304'


def get_poi_ids(rect_dict, keys):
    """
    获取格网范围内的所有POI编号ID值
    :param rect_dict: 格网编号及左上右下坐标
    :param keys: 高德keys
    :return: 返回所有的POI编号ID值
    """
    result = list()
    rect_c = 1
    k = 0
    for g_k, g_v in rect_dict.items():
        print(rect_c)
        key = keys[k % len(keys)]
        path = '{}?key={}&polygon={}&keyword=&output=json&types={}' \
               '&offset=20&page={}&extensions=all'.format(base_api, key, g_v, req_type, 1)
        try:
            requests.adapters.DEFAULT_RETRIES = 5
            requests.session().keep_alive = False
            req_result = requests.get(path, timeout=45).json()
        except Exception as e:
            # print("Exception: {}".format(e))
            return -2
        else:
            r_status = req_result['status']
            r_count = int(req_result['count'])
            if r_status == '1' and r_count > 1:
                page_count = r_count / 20 + 1
                for page_idx in range(1, page_count+1):
                    poi_ids = get_pois_id_in_rect(g_v, page_idx, keys[k % len(keys)], req_type)
                    k += 1
                    if poi_ids != -2:
                        result.extend(poi_ids)
        rect_c += 1
    return result


def get_pois_id_in_rect(rect_coor, page_idx, key, req_type):
    """
    获取某一个矩形范围内的POI
    :param rect_coor: 矩形的坐标范围
    :param page_idx: 页面索引
    :param key: 查询所用的高德Key
    :param req_type: 查询类型，目前为住宅
    :return: 返回矩形范围内POI的编号值
    """
    result = list()
    path = '{}?key={}&polygon={}&keyword=&output=json&types={}' \
           '&offset=20&page={}&extensions=all'.format(base_api, key, rect_coor, req_type,page_idx)
    try:
        requests.adapters.DEFAULT_RETRIES = 5
        requests.session().keep_alive = False
        req_result = requests.get(path, timeout=45).json()
    except Exception as e:
        # print("Exception: {}".format(e))
        return -2
    else:
        r_status = req_result['status']
        if r_status == '1':
            r_pois = req_result['pois']
            for poi in r_pois:
                poi_id = poi['id']
                result.append(poi['id'])

        else:
            return -2

    return result


def get_resi_ids(shp_fn,id_field):
    """
    在已经抓到的错误的居住区数据中获取ID集合
    :param shp_fn:
    :param id_field:
    :return:
    """
    ds = ogr.Open(shp_fn, 0)
    shp_lyr = ds.GetLayer(0)
    result = list()
    k = 1
    for p_feat in shp_lyr:
        rid = p_feat.GetField(id_field)
        result.append(rid)
    del ds
    return result

def get_grid_lurd_pts(shp_fn, id_field):
    """
    获取格网的左上右下坐标点坐标。lurd-left up right down
    :param shp_fn:格网shp数据，polygon类型
    :param Idfield:GID字段
    :return:返回每个格网的左上右下坐标值
    """
    ds = ogr.Open(shp_fn, 0)
    shp_lyr = ds.GetLayer(0)
    result = {}
    k = 1
    for p_feat in shp_lyr:
        gid = p_feat.GetField(id_field)
        geom = p_feat.geometry()
        # print geom
        rings = geom.GetGeometryRef(0)
        lu_pt = CoorTrans.wgs84_to_gcj02(rings.GetPoints()[0][0], rings.GetPoints()[0][1])
        rd_pt = CoorTrans.wgs84_to_gcj02(rings.GetPoints()[2][0], rings.GetPoints()[2][1])
        lurd_pts = '%s,%s;%s,%s' % (lu_pt[0], lu_pt[1], rd_pt[0], rd_pt[1])
        result[gid] = lurd_pts
    del ds
    return result


def get_id_infomation(poi_id):
    path = 'https://ditu.amap.com/detail/get/detail?id={}'
    req = path.format(poi_id)
    try:
        requests.adapters.DEFAULT_RETRIES = 5
        requests.session().keep_alive = False
        req_result = requests.get(req, timeout=45).json()
    except Exception as e:
        # print("Exception: {}".format(e))
        return -2
    else:
        r_status = req_result['status']
        if r_status == '1' and 'mining_shape' in req_result['data']['spec'].keys():
            poi_area = req_result['data']['spec']['mining_shape']['area']
            poi_center = req_result['data']['spec']['mining_shape']['center']
            poi_x = poi_center.split(',')[0]
            poi_y = poi_center.split(',')[1]
            poi_name = req_result['data']['base']['name']
            poi_shape = req_result['data']['spec']['mining_shape']['shape']
            # poi_shape_wkt = "POLYGON ((" + trans_to_wkbString(poi_shape) + "))"
            # print poi_shape_wkb
            result = [poi_id, poi_name, poi_x, poi_y, poi_area, poi_shape] #poi_shape_wkt]# poi_shape]
            return result
        else:
            return -2


def trans_to_wkbString(poi_shape):
    result = list()
    dict_coor = poi_shape.split(';')
    for pt in dict_coor:
        str_newpt = '%s %s' % (float(pt.split(',')[0]), float(pt.split(',')[1]))
        result.append(str_newpt)
    return ','.join(result)


def read_config_csv(csv_fn):
    csv_data = {}
    with open(csv_fn, 'r') as csvfile:
        rec_reader = csv.reader(csvfile, dialect='excel-tab')
        for row_rec in rec_reader:
            csv_data[row_rec[0]] = row_rec[1]
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


def create_polygon_shpfn(feats, out_fn):
    reload(sys)
    sys.setdefaultencoding('utf-8')
    # 注册所有的驱动
    gdal.AllRegister()
    ogr.RegisterAll()
    # 为了支持中文路径，请添加下面这句代码
    gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "NO")
    # 为了使属性表字段支持中文，请添加下面这句
    gdal.SetConfigOption("SHAPE_ENCODING", "")  # CP936

    driver = ogr.GetDriverByName("ESRI Shapefile")
    ds = driver.CreateDataSource(out_fn)
    srs = osr.SpatialReference()
    # srs.ImportFromEPSG(4326)
    layer = ds.CreateLayer("lyr", srs, ogr.wkbPolygon)

    field_name = ogr.FieldDefn("POIID", ogr.OFTString)
    layer.CreateField(field_name)
    field_name = ogr.FieldDefn("NAME", ogr.OFTString)
    layer.CreateField(field_name)
    field_name = ogr.FieldDefn("X", ogr.OFTReal)
    layer.CreateField(field_name)
    field_name = ogr.FieldDefn("Y", ogr.OFTReal)
    layer.CreateField(field_name)
    field_name = ogr.FieldDefn("AREA", ogr.OFTReal)
    layer.CreateField(field_name)

    k = 1
    for feat_v in feats:
        pod_id = feat_v[0]
        poi_name = feat_v[1]
        poi_x = float(feat_v[2])
        poi_y = float(feat_v[3])
        poi_area = float(feat_v[4])
        feature = ogr.Feature(layer.GetLayerDefn())
        feature.SetField('POIID', pod_id)
        feature.SetField('NAME', poi_name.decode('UTF-8').encode('GBK'))
        feature.SetField('X', poi_x)
        feature.SetField('Y', poi_y)
        feature.SetField('AREA', poi_area)

        poi_polygon_wkt = feat_v[5]
        # print poi_polygon_wkt
        # polygon_geo = ogr.CreateGeometryFromWkt(poi_polygon_wkt)
        # print polygon_geo
        polygon_geo = create_polygon_geo(poi_polygon_wkt)

        feature.SetGeometry(polygon_geo)
        layer.CreateFeature(feature)
        feature = None
        print('feature %s of %s' % (k, len(feats)))
        k += 1
    ds = None


def create_polygon_geo(polygon_pts):
    ring = ogr.Geometry(ogr.wkbLinearRing)
    pts = polygon_pts.split(';')

    for pt in pts:
        pt_x = pt.split(',')[0]
        pt_y = pt.split(',')[1]
        ring.AddPoint(float(pt_x), float(pt_y))
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly



if __name__ == '__main__':
    resi_shp_fn = r'E:\garden_data\residential_all.shp'
    rid_list = get_resi_ids(resi_shp_fn,'POIID')
    out_shp_fn = r'E:\garden_data\r_p_all.shp'
    shp_recs = list()
    k = 1
    count1, count2 = 120, 500#3588
    for rid in rid_list:
        print '%s - %s' % (k, len(rid_list))
        if count1 < k < count2: #and k > count1:
            sencond_r = random.randint(45, 90)
            time.sleep(sencond_r)
            t_shpinfo = get_id_infomation(rid)
            if t_shpinfo != -2:
                shp_recs.append(t_shpinfo)
        k += 1
    create_polygon_shpfn(shp_recs, out_shp_fn)
    '''
    print t_shpinfo

    config_fn = r'E:\garden_data\config.csv'
    config_dict = read_config_csv(config_fn)
    in_gridpt_fn = config_dict['in_shp_fn']
    out_shp_fn = config_dict['out_shp_fn']
    in_key_fn = config_dict['in_key_fn']
    keys = read_keys(in_key_fn)

    GID_dict = get_grid_lurd_pts(in_gridpt_fn, 'GID1000')
    pois_id = get_poi_ids(GID_dict, keys)
    pois_shp_recs = dict()
    for poi_id in pois_id:
        poi_shpinfo = get_id_infomation(poi_id)
        print(poi_shpinfo)
        if poi_shpinfo != -2:
            pois_shp_recs[poi_id] = poi_shpinfo
    create_polygon_shpfn(pois_shp_recs, out_shp_fn)
'''
