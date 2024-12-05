@echo off

cd %userprofile%\projects\rwswd\

call venv\Scripts\activate

:: Excute
python %userprofile%\Projects\rwswd\main.py -i sql -o vnthu01

pause