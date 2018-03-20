# -*- coding: UTF-8 -*-
import requests
import urllib
if __name__=='__main__':
    path = 'https://ditu.amap.com/detail/get/detail?id={}'
    add_id = 'B000A83856'
    req = path.format(add_id)
    result = requests.get(req, timeout=45).json()
    b_data = result['data']['spec']['mining_shape']['shape']
    b_data1 = b_data.split(';')
    k = 1
    for pt in b_data1:
        print('%s,%s' % (k, pt))
        k += 1
