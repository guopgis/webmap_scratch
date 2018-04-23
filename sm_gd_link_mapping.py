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


def get_degree(latA, lonA, latB, lonB):
    """
    Args:
        point p1(latA, lonA)
        point p2(latB, lonB)
    Returns:
        bearing between the two GPS points,
        default: the basis of heading direction is north
    """
    radLatA = math.radians(latA)
    radLonA = math.radians(lonA)
    radLatB = math.radians(latB)
    radLonB = math.radians(lonB)
    dLon = radLonB - radLonA
    y = math.sin(dLon) * math.cos(radLatB)
    x = math.cos(radLatA) * math.sin(radLatB) - math.sin(radLatA) * math.cos(radLatB) * math.cos(dLon)
    brng = math.degrees(math.atan2(y, x))
    brng = (brng + 360) % 360
    return brng

def get_sm_links(sm_link_shpfn, gd_link_shpfn):
    """
    读取世脉link的geo和ROADID
    :param sm_link_shpfn:
    :param gd_link_shpfn:
    :return:
    """

    sm_ds = ogr.Open(sm_link_shpfn, 0)
    gd_ds = ogr.Open(gd_link_shpfn, 0)
    sm_link_lyr = sm_ds.GetLayer(0)
    gd_link_lyr = gd_ds.GetLayer(0)

    result = dict()
    k = 1
    id_field = 'ROADID'
    for sm_feat in sm_link_lyr:
        r_geo = sm_feat.GetGeometryRef().Clone()
        r_id = sm_feat.GetField(id_field)
        pt_count = r_geo.GetPointCount()
        pt_from = r_geo.GetPoint(0)
        pt_to = r_geo.GetPoint(pt_count-1)
        line_angle = get_degree(pt_from[1], pt_from[0], pt_to[1], pt_to[0])

        buffer_dist = 0.0002
        buffer_poly = r_geo.Buffer(buffer_dist)
        gd_link_lyr.SetSpatialFilter(buffer_poly)
        inter_count = gd_link_lyr.GetFeatureCount()
        angle_speed = dict()
        for gd_feat in gd_link_lyr:
            gd_feat_geo= gd_feat.GetGeometryRef().Clone()
            p_count = gd_feat_geo.GetPointCount()
            p_from = gd_feat_geo.GetPoint(0)
            p_to = gd_feat_geo.GetPoint(p_count - 1)
            gd_line_angle = get_degree(p_from[1], p_from[0], p_to[1], p_to[0])
            r_name = gd_feat.GetField('NAME')
            r_angle = gd_feat.GetField('ANGLE')
            r_speed = gd_feat.GetField('SPEED')
            angle_diff = get_angle_diff(line_angle, gd_line_angle)
            angle_speed[angle_diff] = r_speed
        if len(angle_speed) > 0:
            v_keys = angle_speed.keys()
            v_keys.sort()
            result[r_id] = angle_speed[v_keys[0]]
        else:
            print r_id
            result[r_id] = -99
    del sm_ds
    sorted_dict_keys(result)
    return result


def sorted_dict_keys(adict):
    keys = adict.keys()
    keys.sort()
    return map(adict.get, keys)


def get_angle_diff(angle1, angle2):
    dif = int(abs(angle1 - angle2))
    if dif > 180:
        dif = 360-dif
    return dif


def get_smlink_traffic(sm_link_shpfn, gd_link_shp_path):
    result = dict()
    gd_link_shpfns = os.listdir(gd_link_shp_path)
    for f in gd_link_shpfns:
        if os.path.splitext(f)[1] == '.shp':
            f_name = os.path.splitext(f)[0]
            traffic_dt = f_name.split('_')[3]
            gd_link_shpfn = os.path.join(gd_link_shp_path, f)
            result[traffic_dt] = get_sm_links(sm_link_shpfn, gd_link_shpfn)
    return result

if __name__ == '__main__':
    sm_link_shpfn = r'D:\W03_Shimai\Gd_Traffic\cb_links\road_201704_ld.shp'
    gd_link_shp_path = r'D:\W03_Shimai\Gd_Traffic\out_shpfn'
    sm_traffic_info = get_smlink_traffic(sm_link_shpfn, gd_link_shp_path)
