import argparse
import os
import psycopg
import csv
import logging
from datetime import datetime, timedelta
import sqlite3

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

db_file = 'data.db'

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
    
def initialize_database(logger):
    try:
        if not os.path.exists(db_file):
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            # Create tables and insert initial data here
            cursor.execute('create table if not exists run_dates (id integer primary key autoincrement, lastrun_date text)'
            )

            # Insert initial data
            cursor.execute(f"insert into run_dates (lastrun_date) values ('{datetime.now().strftime('%Y%m%d')}')")

            conn.commit()
            conn.close()
            logger.info("Database initialized successfully.")
    except Exception as e:
            logger.error(e)

def get_lastrun_date():
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('select lastrun_date from run_dates where id=1')
    lastrun_date = cursor.fetchall().pop()[0]
    conn.commit()
    conn.close()
    return lastrun_date

def update_lastrun_date(date):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(f"update run_dates set lastrun_date = '{date}' where id=1")
    conn.commit()
    conn.close()



def append_to_outfile(filename, rows):
    with open(filename, 'a', encoding='utf-8') as f_out:
        output = csv.writer(f_out, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONE, escapechar='\\')
        output.writerows(rows)

def get_sql_statement_from_file(filename):
    sql = ''
    with open(filename, 'r') as f_in:
        for line in f_in:
            sql += line
    return sql

def parseArguments():
    # Create argument parser
    parser = argparse.ArgumentParser()

    # Optional arguments
    parser.add_argument("-i", "--sql", help="sql", type=str, default='sql')
    parser.add_argument("-o", "--csv", help="csv", type=str, required=True, default=None)
    
    # Print version
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')

    # Parse arguments
    args = parser.parse_args()

    return args

def extract_data_to_file(sql_fn, city_code, logger):

    header_flag = 0
    sql_filename = f"{os.getcwd()}\\{sql_fn}.sql"
    sql_statement = get_sql_statement_from_file(sql_filename)

    while True:
        run_date = datetime.strptime(get_lastrun_date(), '%Y%m%d') + timedelta(days=1)
        if datetime.strftime(run_date,'%Y%m%d') < datetime.strftime(datetime.now(),'%Y%m%d'):
            out_filename = f"{os.getcwd()}\\{city_code}_{datetime.strftime(run_date,'%Y%m%d')}"
            logger.info(f"Start {out_filename}")
            try:
                #encoding="US-ASCII" ISO-8859-1
                with psycopg.connect(**CONN_INFO) as connection:       
                    with connection.cursor() as cursor:
                        cursor.arraysize = 1000
                        cursor.execute(sql_statement)

                        header_cols = []
                        for col in cursor.description:
                            header_cols.append(col[0])
                            
                        if header_flag == 0:
                            append_to_outfile(f"{out_filename}.csv", [header_cols])
                            header_flag = 1
                            
                        rows = cursor.fetchall()
                        append_to_outfile(f"{out_filename}.csv", rows)
                update_lastrun_date(datetime.strftime(run_date,'%Y%m%d'))
                logger.info("Successfully")
            except Exception as e:
                logger.error(f"Something error occurred: {e}")

            logger.info(f"End {out_filename}")
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

    initialize_database(logger)

    get_lastrun_date()

    extract_data_to_file(_sql, _csv, logger)

