#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time       : 2023/12/2 21:07
# @Author     : 落狼
# @File       : auto_cds_downloader.py
# @Environment: Python 3.9.1.2
# @Software   : PyCharm

import os
import cdsapi
import datetime
from subprocess import Popen
from joblib import Parallel, delayed

# ...（其余导入和函数定义与原代码相同）...

def idmDownloader(task_url, folder_path, file_name):
    """
    IDM下载器
    :param task_url: 下载任务地址
    :param folder_path: 存放文件夹
    :param file_name: 文件名
    :return:
    """
    # IDM安装目录
    idm_engine = "C:\\Program Files (x86)\\Internet Download Manager\\IDMan.exe"

    # # 将任务添加至队列
    # call([idm_engine, '/d', task_url, '/p', folder_path, '/f', file_name, '/a'])
    # # 开始任务队列
    # call([idm_engine, '/s'])

    # Asynchronous Run
    Popen([idm_engine, '/d', task_url, '/p', folder_path, '/f', file_name, '/a'])
    Popen([idm_engine, '/s'])

if __name__ == '__main__':
    c = cdsapi.Client()  # 创建用户


    dic = {
            'variable': [
                '10m_u_component_of_wind',
                '10m_v_component_of_wind',
                '2m_dewpoint_temperature',
                '2m_temperature', 'evaporation_from_bare_soil', 'evaporation_from_open_water_surfaces_excluding_oceans',
                'evaporation_from_the_top_of_canopy', 'evaporation_from_vegetation_transpiration', 'forecast_albedo',
                'leaf_area_index_high_vegetation', 'leaf_area_index_low_vegetation', 'potential_evaporation',
                'runoff', 'skin_reservoir_content', 'skin_temperature',
                'snow_evaporation',
                'soil_temperature_level_1',
                'soil_temperature_level_2',
                'soil_temperature_level_3', 'soil_temperature_level_4',
                'sub_surface_runoff',
                'surface_latent_heat_flux', 'surface_net_solar_radiation', 'surface_net_thermal_radiation',
                'surface_pressure', 'surface_runoff', 'surface_sensible_heat_flux',
                'surface_solar_radiation_downwards', 'surface_thermal_radiation_downwards', 'total_evaporation',
                'total_precipitation', 'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2',
                'volumetric_soil_water_layer_3', 'volumetric_soil_water_layer_4',
            ],
            'year': '',
            'month': '',
            'day': [],
            'time': [
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
            'area': [
                60, 70, 0,
                140,
            ],
            'format': 'netcdf.zip',
        }

    start_date = datetime.date(2009, 1, 1)  # 设置起始日期
    end_date = datetime.date(2016, 12, 31)  # 设置结束日期

    # 计算总的日期列表
    total_date_list = [start_date + datetime.timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    # 读取已下载的文件
    dl_dir = 'D:\\DOWNLOAD\\08_'
    downloaded_files = os.listdir(dl_dir)  # 修改为您的路径
    downloaded_dates = [datetime.datetime.strptime(file.split('.')[0], '%Y%m%d').date() for file in downloaded_files]

    # 从总列表中去除已下载的日期
    date_list = [date for date in total_date_list if date not in downloaded_dates]

    def download_data(dt):
        y = dt.strftime('%Y')
        m = dt.strftime('%m')
        d = dt.strftime('%d')

        dic['year'] = y
        dic['month'] = m.zfill(2)
        dic['day'] = d.zfill(2)

        dt_str = dt.strftime('%Y%m%d')

        print("REQUEST- " + dt_str)

        flag = True
        while flag:
            try:
                r = c.retrieve('reanalysis-era5-land', dic)

                path = dl_dir
                if not os.path.exists(path):
                    os.mkdir(path)

                filename = f'{dt_str}.zip'
                print("DOWNLOAD- " + dt_str)
                idmDownloader(r.location, path, filename)

                print(r.location)
                flag = False
            except:
                print('________ERROR LOOP______________')

    # 使用joblib实现多线程下载
    Parallel(n_jobs=4)(delayed(download_data)(dt) for dt in date_list)  # 可以调整n_jobs来设置线程数量
