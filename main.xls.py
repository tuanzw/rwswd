import pandas as pd
import argparse
import os
import csv
from datetime import datetime
import paramiko
import glob
import shutil
import win32com.client as win32
import re
from pathlib import PurePath
from dotenv import dotenv_values

from logapi import logger

import pdb


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

def save_mail_attachemnt(city_code: str):
    outlook = win32.Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.GetDefaultFolder(6)
    # if env.get('email_folder') found, work on it else work on Inbox
    working_folder = inbox
    for folder in inbox.Folders:
        if folder.name == env.get('email_folder'):
            working_folder = folder
            break
    # Stop if there is no email_move_to_folder existed
    try:
        to_folder = working_folder.Folders(env.get('email_move_to_folder'))
    except Exception as e:
        print(
            f"Please create {env.get('email_move_to_folder')} as subfolder of {working_folder.Name} in the mailbox"
        )
        logger(
            f"Please create {env.get('email_move_to_folder')} as subfolder of {working_folder.Name} in the mailbox"
        )
        exit()

    logger.info(f"Working on folder {working_folder.Name}")

    # Sound goood, working on emails received in folder
    # https://learn.microsoft.com/en-us/office/vba/outlook/how-to/search-and-filter/filtering-items-using-a-date-time-comparison
    messages = working_folder.Items
    to_move_messages = []
    for message in messages:
        mail_subject = message.Subject
        # if not a email or subject pattern is not valid, skip
        if message.Class != 43:
            continue

        if re.search(env.get('mail_subject_pattern'), mail_subject, re.IGNORECASE) is None:
            msg = f"__SKIP__:{mail_subject}__Not well-formed Mail Subject"
            logger.info(msg)
            continue
        if not mail_subject.split('_')[0].lower() == city_code:
            msg = f"__SKIP__:{mail_subject}__{mail_subject.split('_')[0]} not a valid site"
            logger.info(msg)
            print(msg)
            continue
        # proceed to save .xlsx files in attachment of valid mail
        to_move_messages.append(message)
        attachments = message.Attachments
        for attachment in attachments:
            if attachment.FileName.split(".")[-1] == "xls":
                file_path = PurePath(os.getcwd(), f'{city_code}_{attachment.FileName}')
                attachment.SaveAsFile(file_path)
    # all good, move proceeded email to email_move_to_folder
    for message in to_move_messages:
        message.Move(to_folder)
        print(f"__PROCEEDED__:{message.Subject}")

def extract_data_to_file(input_fn):
    city_code, rest = fn.split('_')
    wdate = rest[16:26]
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
    _csv = args.csv.strip()

    try:
        logger.info('Start')

        save_mail_attachemnt(city_code=_csv)

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