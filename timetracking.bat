@echo off

call %userprofile%\projects\rwswd\venv\Scripts\activate

:: Excute
python %userprofile%\Projects\rwswd\main.py -i sql -o vnthu01

pause