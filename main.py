import argparse
import os
import psycopg
import csv
import logging
from datetime import datetime, timedelta
import sqlite3
import paramiko
import glob
import shutil

import polars as pl
from sqlalchemy import create_engine


from dotenv import dotenv_values


env = dotenv_values('.env')

CONN_INFO = {
    'host': env.get('dbhost'),
    'port': env.get('dbport'),
    'user': env.get('dbuser'),
    'password': env.get('dbpwd'),
    'dbname': env.get('dbname'),
    'sslrootcert': env.get('ca_cert_file'),
    'sslmode' : 'verify-full'
}

ssl_args = {
    'sslmode': 'verify-full',
    'sslrootcert': env.get('ca_cert_file'),
}

conn_str = f"postgresql+psycopg://{env.get('dbuser')}:{env.get('dbpwd')}@{env.get('dbhost')}:{env.get('dbport')}/{env.get('dbname')}"


db_file = 'data.db'

workday_csv = 'workdayid.csv'

class CustomFormatter(logging.Formatter):
    """Logging colored formatter, adapted from https://stackoverflow.com/a/56944256/3638629"""
    grey = '\x1b[38;21m'
    blue = '\x1b[38;5;39m'
    yellow = '\x1b[38;5;226m'
    red = '\x1b[38;5;196m'
    bold_red = '\x1b[31;1m'
    reset = '\x1b[0m'

    def __init__(self, fmt):
        super().__init__()
        self.fmt = fmt
        self.FORMATS = {
            logging.DEBUG: self.grey + self.fmt + self.reset,
            logging.INFO: self.blue + self.fmt + self.reset,
            logging.WARNING: self.yellow + self.fmt + self.reset,
            logging.ERROR: self.red + self.fmt + self.reset,
            logging.CRITICAL: self.bold_red + self.fmt + self.reset
        }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
    
def initialize_database():
    if not os.path.exists(db_file):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        # Create tables and insert initial data here
        cursor.execute('create table if not exists run_dates (id integer primary key autoincrement, lastrun_date text)')
        # Insert initial data
        cursor.execute(f"insert into run_dates (lastrun_date) values ('{datetime.now().strftime('%Y%m%d')}')")
        conn.commit()
        conn.close()

def get_lastrun_date():
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('select lastrun_date from run_dates where id=1')
    lastrun_date = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    return lastrun_date

def update_lastrun_date(date):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(f"update run_dates set lastrun_date = '{date}' where id=1")
    conn.commit()
    conn.close()

def get_sql_statement_from_file(filename):
    sql = ''
    with open(filename, 'r') as f_in:
        for line in f_in:
            sql += line
    return sql

def sftp_upload(host, port, username, password, filenames, remote_folder):
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
    parser.add_argument("-i", "--sql", help="sql", type=str, default='sql')
    parser.add_argument("-o", "--csv", help="csv", type=str, required=True, default=None)
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')
    args = parser.parse_args()
    return args

def extract_data_to_file(sql_fn, city_code):
    sql_filename = f"{os.getcwd()}\\{sql_fn}.sql"
    sql_statement = get_sql_statement_from_file(sql_filename)

    while True:
        run_date = datetime.strptime(get_lastrun_date(), '%Y%m%d') + timedelta(days=1)
        if datetime.strftime(run_date,'%Y%m%d') < datetime.strftime(datetime.now(),'%Y%m%d'):
            out_filename = f"{os.getcwd()}\\{city_code}_{datetime.strftime(run_date,'%Y%m%d')}"
            try:
                if env.get('environment') == 'uat':
                    engine = create_engine(conn_str,connect_args=ssl_args)
                else: # prod
                    engine = create_engine(conn_str)
                event_df = pl.read_database(query=sql_statement, connection=engine,
                                            execute_options={"parameters": {"wdate": datetime.strftime(run_date,'%Y%m%d') },})
                emp_df = pl.read_csv(source=workday_csv, has_header=True)
                with pl.SQLContext(event=event_df, eager=True) as ctx:
                    try:
                        ctx.register_many(employee=emp_df)
                        result = ctx.execute("SELECT 'VNM_Regular_Hours' time_entry_code, event.clock_event_type, employee.workday_worker_id, event.timezone, event.datetime FROM employee inner join event on employee.payroll_id=event.payroll_id")
                        result.write_csv(f"{out_filename}.csv", include_header=True)
                    except Exception as e:
                        logger.error(e)
                update_lastrun_date(datetime.strftime(run_date,'%Y%m%d'))
            except Exception as e:
                break
        else:
            break
                        
if __name__=='__main__':
    # Parse the arguments
    args = parseArguments()
    _sql = args.sql.split('.')[0]
    _csv = args.csv

    # Create custom logger logging all five levels
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Define format for logs
    fmt = '%(asctime)s | %(levelname)8s | %(message)s'

    # Create stdout handler for logging to the console (logs all five levels)
    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(logging.ERROR)
    stdout_handler.setFormatter(CustomFormatter(fmt))

    # Create file handler for logging to a file (logs all five levels)
    today = datetime.now()
    file_handler = logging.FileHandler(f"{today.strftime('%Y%m%d')}.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(fmt))

    # Add both handlers to the logger
    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

    try:
        logger.info('Start')
        initialize_database()
        extract_data_to_file(_sql, _csv)
        sftp_upload(host=env.get('sftp_host'), port=env.get('sftp_port'),
                username=env.get('sftp_username'), password=env.get('sftp_password'),
                remote_folder=env.get('sftp_remote_folder'), filenames=glob.glob(f'{_csv}*.csv'))
        logger.info('End successfully')
    except Exception as e:
        logger.error(e)

