#!/usr/bin/python3

import argparse
import json
import os
import pytz
from datetime import datetime
from datetime import timedelta

from curwmysqladapter import MySQLAdapter, Station
from Utils import extract_n_push_precipitation, extract_n_push_temperature

def utc_to_sl(utc_dt):
    sl_timezone = pytz.timezone('Asia/Colombo')
    return utc_dt.replace(tzinfo=pytz.utc).astimezone(tz=sl_timezone)

try:
    ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
    COMMON_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    forceInsert = False

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='Configuration file that includes db configs and stations. Default is ./CONFIG.json.')
    parser.add_argument('-f', '--force', action='store_true', help='Enables force insert.')
    args = parser.parse_args()

    print('\n\nCommandline Options:', args)

    if args.config:
        CONFIG = json.loads(open(os.path.join(ROOT_DIR, args.config)).read())
    else:
        CONFIG = json.loads(open(os.path.join(ROOT_DIR, './CONFIG.json')).read())
    forceInsert = args.force

    stations = CONFIG['stations']

    extract_from_db = CONFIG['extract_from']
    push_to_db = CONFIG['push_to']

    extract_adapter = MySQLAdapter(
        host=extract_from_db['MYSQL_HOST'],
        user=extract_from_db['MYSQL_USER'],
        password=extract_from_db['MYSQL_PASSWORD'],
        db=extract_from_db['MYSQL_DB'])
    push_adapter = MySQLAdapter(
        host=push_to_db['MYSQL_HOST'],
        user=push_to_db['MYSQL_USER'],
        password=push_to_db['MYSQL_PASSWORD'],
        db=push_to_db['MYSQL_DB'])

    # Prepare start and date times.
    now_date = utc_to_sl(datetime.now())
    # now_date = datetime.now()
    start_datetime_obj = now_date - timedelta(minutes=15)
    end_datetime_obj = now_date
    start_datetime = start_datetime_obj.strftime(COMMON_DATE_FORMAT)
    end_datetime = end_datetime_obj.strftime(COMMON_DATE_FORMAT)

    # start_datetime = '2018-02-06 23:50:00'
    # end_datetime = '2018-02-07 23:59:59'

    for station in stations:
        print("**************** Station: %s, start_date: %s, end_date: %s **************"
              % (station['name'], start_datetime, end_datetime))
        try:
            extract_n_push_precipitation(extract_adapter, push_adapter, station, start_datetime, end_datetime)
        except Exception as ex:
            print("Error occured while pushing precipitation.", ex)
        try:
            extract_n_push_temperature(extract_adapter, push_adapter, station, start_datetime, end_datetime)
        except Exception as ex:
            print("Error occured while pushing temperature.", ex)

except Exception as ex:
    print('Error occurred while extracting and pushing data:', ex)

