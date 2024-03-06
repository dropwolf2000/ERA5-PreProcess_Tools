#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time       : 2023/12/22 16:22
# @Author     : 落狼
# @File       : unzip_rename_netcdfZIP.py
# @Environment: Python 3.9.1.2
# @Software   : PyCharm
import os
import zipfile
from joblib import Parallel, delayed
import shutil
import uuid

def extract_and_rename_single(zip_path, output_folder):
    # print(zip_path)
    # 使用随机生成的目录名避免命名冲突
    try:

        temp_dir = os.path.join(output_folder, str(uuid.uuid4()))
        os.makedirs(temp_dir, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # 提取所有文件到临时目录
            zip_ref.extractall(temp_dir)

        # 构造新文件名
        new_name = os.path.basename(zip_path).replace('.zip', '.nc')
        original_nc_path = os.path.join(temp_dir, 'data.nc')
        new_nc_path = os.path.join(output_folder, new_name)

        # 重命名文件
        os.rename(original_nc_path, new_nc_path)

        shutil.rmtree(temp_dir)

    except Exception as e:
        print(zip_path)
        raise Exception(str(e))


    # 返回新文件路径
    return new_nc_path

def extract_and_rename_parallel(zip_folder, output_folder):
    # 确保输出文件夹存在
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # 遍历指定文件夹中的所有zip文件
    zip_files = [file for file in os.listdir(zip_folder) if file.endswith('.zip')]
    zip_paths = [os.path.join(zip_folder, file) for file in zip_files]

    # 并行处理
    Parallel(n_jobs=12)(delayed(extract_and_rename_single)(zip_path, output_folder) for zip_path in zip_paths)


if __name__ == '__main__':

    # 使用示例
    zip_folder = r'H:\ERA5\10'  # 替换为包含zip文件的文件夹路径
    output_folder = 'D:\PROJECTS\PRECIPITATION_CLASSIFY\ERA5_hourly'  # 替换为输出文件的文件夹路径
    extract_and_rename_parallel(zip_folder, output_folder)