@echo off
call C:\ProgramData\Anaconda3\Scripts\activate.bat C:\ProgramData\Anaconda3
cd %~dp0
python auto_cds_downloader.py
pause
