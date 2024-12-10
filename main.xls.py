import pandas as pd
import argparse
import os
import csv
import logging
from datetime import datetime
import paramiko
import glob
import shutil
from dotenv import dotenv_values

from logapi import logger


env = dotenv_values('.env')

record = {
'time_entry_code' : 'VNM_Regular_Hours',
'clock_event_type' : 'IN',
'workday_worker_id' : '',
'timezone' : 'Asia/Jarkata',
'datetime' : ''
}

workday_csv = 'workdayid.csv'

personel_id = 2
first_name = 3
last_name = 4
record_date = 5
earliest_time = 6
latest_time = 7

def sftp_upload(host, port, username, password, filenames, remote_folder):
    if env.get('environment') != 'uat':
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=host, port=port, username=username, password=password, look_for_keys=False)
            sftp = ssh.open_sftp()
            sftp.chdir(remote_folder)
            for filename in filenames:
                sftp.put(localpath=f"{os.getcwd()}\\{filename}", remotepath=filename)
                shutil.move(src=f"{os.getcwd()}\\{filename}", dst=f"{os.getcwd()}\\csv\\{filename}")

def parseArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--csv", help="csv", type=str, required=True, default=None)
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')
    args = parser.parse_args()
    return args


def get_workdayid(ref_id, wd_dataframe: pd.DataFrame) -> str:
    data = wd_dataframe[wd_dataframe["payroll_id"] == ref_id]
    if data.shape[0] > 0:
        return data["workday_worker_id"].iloc[0]
    else:
        return None
    
def write_rows_to_csv(rows, filename):
    fieldnames = rows[0].keys()
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def extract_data_to_file(input_fn):
    city_code, wdate, _ = fn.split('_')
    wdate = datetime.strftime(datetime.strptime(wdate, '%Y-%m-%d'), '%Y%m%d')
    zk_df = pd.read_excel(input_fn, sheet_name=0, header=0,
                              converters={earliest_time: str, latest_time: str})
    wd_df = pd.read_csv(workday_csv, header=0)

    rows = []
    for event in zk_df.itertuples(index=False):
        workday_worker_id = get_workdayid(event[last_name], wd_dataframe=wd_df)
        if workday_worker_id:
            in_event = {**record}
            out_event = {**record}

            in_event_datetime = f'{event[record_date]}{datetime.strftime(datetime.strptime(event[earliest_time], '%H%M%S'), 'T%H:%M:%S')}'
            out_event_datetime = f'{event[record_date]}{datetime.strftime(datetime.strptime(event[latest_time], '%H%M%S'), 'T%H:%M:%S')}'

            in_event.update({'datetime': in_event_datetime,'workday_worker_id': workday_worker_id})
            out_event.update({'clock_event_type': 'OUT', 'datetime': out_event_datetime, 'workday_worker_id': workday_worker_id})
            rows.append(in_event)
            rows.append(out_event)

    write_rows_to_csv(rows, f'{city_code}_{wdate}.csv')


if __name__ == '__main__':

    # Parse the arguments
    args = parseArguments()
    _csv = args.csv

    try:
        logger.info('Start')
        input_filenames = glob.glob(f'{_csv}*.xls')
        for fn in input_filenames:
            extract_data_to_file(fn)
            sftp_upload(host=env.get('sftp_host'), port=env.get('sftp_port'),
                    username=env.get('sftp_username'), password=env.get('sftp_password'),
                    remote_folder=env.get('sftp_remote_folder'), filenames=glob.glob(f'{_csv}*.csv'))
            shutil.move(src=f"{os.getcwd()}\\{fn}", dst=f"{os.getcwd()}\\csv\\{fn}")
            logger.info(f'Proceeded: {fn}')
        logger.info('End successfully')
    except Exception as e:
        logger.error(e)