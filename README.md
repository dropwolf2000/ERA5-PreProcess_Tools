# ERA5-PreProcess_Tools
用于处理ERA5下载的原始NC数据 

***

### 使用说明

1. src/downloader 目录下是era5批量下载工具 支持多线程同时下载
2. src/preprocess 目录下是era5预处理工具 
   1. unzip_rename_netcdfZIP.py 用于批量解压下载好的压缩包并重命名（如果在CDS请求时选用zip压缩格式）
   2. era5_hourly_to_daily.ipynb 用于hourly尺度的ERA5数据合并成daily

