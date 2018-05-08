# -*- coding:utf-8 –*-
import sys
import os
import csv
import math
import numpy as np
from osgeo import ogr, osr
from guop import shpfile_io
import guop.coordinate as Coor


def get_grid_pt_coor(pt_shpfn):
    result = dict()
    ds_grid_pt = ogr.Open(pt_shpfn, 0)
    lyr_grid_pt = ds_grid_pt.GetLayer(0)
    for pt_feat in lyr_grid_pt:
        pt_id = pt_feat.GetField('GID100')
        pt_x = pt_feat.geometry().GetX()
        pt_y = pt_feat.geometry().GetY()
        result[pt_id] = Coor.wgs84_to_gcj02(pt_x, pt_y)

    return result


def get_grid_have_station():
    result = dict()
    result_list = list()

    in_grid_shpfn = r'D:\W02_Paper\P005_Pubtrans_Analysis\pubtrans_data\study_grid100m_pg.shp'
    in_staition_shpfn = r'D:\W02_Paper\P005_Pubtrans_Analysis\pubtrans_data\study_pubstation.shp'
    ds_grid = ogr.Open(in_grid_shpfn, 0)
    ds_station = ogr.Open(in_staition_shpfn, 0)
    lyr_grid = ds_grid.GetLayer(0)
    lyr_station = ds_station.GetLayer(0)

    for feat in lyr_grid:
        geom = feat.geometry()
        g_id = feat.GetField('GID100')
        # street_pop = street_feat.GetField('POP_2016')
        lyr_station.SetSpatialFilter(geom)
        s_count = lyr_station.GetFeatureCount()
        if s_count != 0:
            result_list.append(g_id)
    return result_list


def export_result_2(csv_fn, result_dict):
    with open(csv_fn, 'wb') as out_csvfile:
        writer = csv.writer(out_csvfile)
        writer.writerow(['from_gid', 'from_x','from_y','to_gid','to_x','to_y'])
        k = 1
        for d_k, d_v in result_dict.items():
            w_line = d_v
            writer.writerow(w_line)
            if k %1000 == 0:
                print '%s of %s' % (k, len(result_dict))
            k += 1


def get_scratch_od(grids_list):
    in_gridpt_shpfn = r'D:\W02_Paper\P005_Pubtrans_Analysis\pubtrans_data\study_grid100m_pt.shp'
    grid_pt_dict = get_grid_pt_coor(in_gridpt_shpfn)

    # k = 0
    result2 = dict()
    g_count = len(grids_list)
    for i in range(0, g_count):
        print('%s-%s'%(i+1, g_count))
        from_gid = grids_list[i]
        for j in range(0, g_count):
            to_grid = grids_list[j]
            if to_grid != from_gid:
                line = '%s_%s' % (from_gid, to_grid)
                result2[line] = [from_gid] + grid_pt_dict[from_gid] + [to_grid] + grid_pt_dict[to_grid]
        # k += 1
        # if k >= 100:
        #     break
    csv_fn = r'D:\W02_Paper\P005_Pubtrans_Analysis\pubtrans_data\scratch_od_lines.csv'
    export_result_2(csv_fn, result2)


if __name__ == '__main__':
    grids_list_has_station = get_grid_have_station()
    get_scratch_od(grids_list_has_station)






