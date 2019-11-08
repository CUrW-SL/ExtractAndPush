import copy
import decimal

from curwmysqladapter import TimeseriesGroupOperation, Station, Data

timeseries_meta_struct = {
    'station': '',
    'variable': '',
    'unit': '',
    'type': '',
    'source': '',
    'name': ''
}


def _precipitation_timeseries_processor(timeseries, _=None):
    if timeseries is None or len(timeseries) <= 0:
        return []
    #Max value for precipitation in 100 years for 5 minute time interval
    qualitControl = 41.63
    index = 0
    new_timeseries = []
    while (index+1) < len(timeseries):
        datetime = timeseries[index+1][0]
        instantaneous_percipitation = timeseries[index+1][1] - timeseries[index][1]

        value = 0 if instantaneous_percipitation < 0 else instantaneous_percipitation
        new_timeseries.append([datetime, value])
        #value = 0 if instantaneous_percipitation < 0 else instantaneous_percipitation
        #if instantaneous_percipitation < 0:
        #    value = 0

        #elif 0 < instantaneous_percipitation < qualitControl:
        #    value = instantaneous_percipitation

        #elif instantaneous_percipitation > qualitControl:
        #    value = None

        #else:
        #    value = None

        #new_timeseries.append([datetime, value])

        index += 1

    return new_timeseries


def _waterlevel_timeseries_processor(timeseries, mean_sea_level=None, waterLevel_min=None, waterLevel_max=None):
    # print("**_waterlevel_timeseries_processor**")
    # print(timeseries)
    if timeseries is None or len(timeseries) <= 0:
        return []

    if mean_sea_level is None or not isinstance(mean_sea_level, (float, int)):
        raise ValueError('Invalid mean_sea_level. Should be a real number.')

    new_timeseries = []
    if decimal.Decimal(mean_sea_level) > 30.00:
        for tms_step in timeseries:
            wl = decimal.Decimal(mean_sea_level) - tms_step[1]
            # Waterlevel should be in between -1 and 3
            if decimal.Decimal(waterLevel_min) <= wl <= decimal.Decimal(waterLevel_max):
                new_timeseries.append([tms_step[0], wl])
    else:
        for tms_step in timeseries:
            wl = decimal.Decimal(mean_sea_level) - tms_step[1]
            # Waterlevel should be in between -1 and 3
            if decimal.Decimal(waterLevel_min) <= wl <= decimal.Decimal(waterLevel_max):
                new_timeseries.append([tms_step[0], wl])
    # print("New Timeseries:")
    # print(new_timeseries)
    return new_timeseries


def _extract_n_push(extract_adapter, push_adapter, station, start_date, end_date, timeseries_meta, group_operation,
                    timeseries_processor=None, **timeseries_processor_kwargs):
    # If there is no timeseries-id in the extracting DB then just return without doing anything.

    timeseries_id = extract_adapter.get_event_id(timeseries_meta)
    # print("*****************")
    # print(timeseries_id)
    # print(start_date)
    # print(end_date)
    # print(group_operation)
    # print("*****************")
    if timeseries_id is None:
        print("No timeseries for the Precipitation of station_Id: %s in the extracting DB."
              % station['stationId'])
        return False

    timeseries = []
    if timeseries_processor is not None:
        # print("1")
        timeseries = timeseries_processor(
            extract_adapter.extract_grouped_time_series(timeseries_id, start_date, end_date, group_operation),
            **timeseries_processor_kwargs
        )
        # print(timeseries)
    else:
        # print("2")
        timeseries = extract_adapter.extract_grouped_time_series(timeseries_id, start_date, end_date, group_operation)
        # print(timeseries)

    # print(timeseries,list)
    if not isinstance(timeseries, list) or len(timeseries) <= 0:
        print("No value in the timeseries for the %s of station_Id: %s in the extracting DB."
              % (timeseries_meta['variable'], station['stationId']))
        return False

    # Check whether there is a timeseries-id in the pushing DB.
    # If not create a timeseried-id in the DB. Else push the extracted timeseries to the pushing DB.
    timeseries_id = push_adapter.get_event_id(timeseries_meta)
    if timeseries_id is None:
        print("No timeseries for the %s of station_Id: %s in the pushing DB."
              % (timeseries_meta['variable'], station['stationId']))

        # Before creating timeseries-id check whether station exists in the DB. If not create a station.
        station_details_in_db = push_adapter.get_station({'stationId': station['stationId'], 'name': station['name']})
        if station_details_in_db is None:
            print("Station: {stationId: %s, name: %s} does not exist in the pushing DB."
                  % (station['stationId'], station['name']))
            if 'station_meta' in station:
                station_meta = station['station_meta']
                station_meta.insert(0, Station.CUrW)
                row_count = push_adapter.create_station(station_meta)
                if row_count > 0:
                    print("Created new station: ", station_meta)
                else:
                    print("Unable to create station: ", station_meta)
                    return False  # Exit on station creation failure.
            else:
                print("Cannot create station. No station_meta in station: %s." % station['name'])
                return False  # Exit on station creation failure.

        # At this point station exists but there is no timeseries-id for this timeseries.
        # Create timeseries-id and insert the timeseries.
        print("Creating timeseries-id in the pushing DB...")
        timeseries_id = push_adapter.create_event_id(timeseries_meta)

        # Insert the extracted timeseries to the pushing DB.
        print("Pushing the extracted timeseries: %s to the pushing DB..." % timeseries_id)
        inserted_rows = push_adapter.insert_timeseries(timeseries_id, timeseries, True, Data.data)
        print("Inserted %d rows from %s tmieseries values successfully..." % (inserted_rows, len(timeseries)))
    else:
        # Insert the extracted timeseries to the pushing DB.
        print("Pushing the extracted timeseries: %s to the pushing DB..." % timeseries_id)
        inserted_rows = push_adapter.insert_timeseries(timeseries_id, timeseries, True, Data.data)
        print("Inserted %d rows from %s tmieseries values successfully..." % (inserted_rows, len(timeseries)))


def extract_n_push_precipitation(extract_adapter, push_adapter, station, start_date, end_date):

    # Create even metadata. Event metadata is used to create timeseries id (event_id) for the timeseries.
    timeseries_meta = copy.deepcopy(timeseries_meta_struct)
    timeseries_meta['station'] = station['name']
    timeseries_meta['variable'] = 'Precipitation'
    timeseries_meta['unit'] = 'mm'
    timeseries_meta['type'] = station['type']
    timeseries_meta['source'] = station['source']
    timeseries_meta['name'] = station['run_name']

    print("#############Extracting and Pushing Precipitation of Station: %s###############" % station['name'])

    _extract_n_push(
        extract_adapter,
        push_adapter,
        station,
        start_date,
        end_date, timeseries_meta,
        TimeseriesGroupOperation.mysql_5min_max,
        _precipitation_timeseries_processor)


def extract_n_push_temperature(extract_adapter, push_adapter, station, start_date, end_date):
    # Create even metadata. Event metadata is used to create timeseries id (event_id) for the timeseries.
    timeseries_meta = copy.deepcopy(timeseries_meta_struct)
    timeseries_meta['station'] = station['name']
    timeseries_meta['variable'] = 'Temperature'
    timeseries_meta['unit'] = 'oC'
    timeseries_meta['type'] = station['type']
    timeseries_meta['source'] = station['source']
    timeseries_meta['name'] = station['run_name']

    print("#############Extracting and Pushing Temperature of Station: %s###############" % station['name'])

    _extract_n_push(
        extract_adapter,
        push_adapter,
        station,
        start_date,
        end_date,
        timeseries_meta,
        TimeseriesGroupOperation.mysql_5min_avg)

def extract_n_push_windspeed(extract_adapter, push_adapter, station, start_date, end_date):
    # Create even metadata. Event metadata is used to create timeseries id (event_id) for the timeseries.
    timeseries_meta = copy.deepcopy(timeseries_meta_struct)
    timeseries_meta['station'] = station['name']
    timeseries_meta['variable'] = 'WindSpeed'
    timeseries_meta['unit'] = 'm/s'
    timeseries_meta['type'] = station['type']
    timeseries_meta['source'] = station['source']
    timeseries_meta['name'] = station['run_name']

    print("#############Extracting and Pushing WindSpeed of Station: %s###############" % station['name'])

    _extract_n_push(
        extract_adapter,
        push_adapter,
        station,
        start_date,
        end_date,
        timeseries_meta,
        TimeseriesGroupOperation.mysql_5min_avg)


def extract_n_push_windgust(extract_adapter, push_adapter, station, start_date, end_date):
    # Create even metadata. Event metadata is used to create timeseries id (event_id) for the timeseries.
    timeseries_meta = copy.deepcopy(timeseries_meta_struct)
    timeseries_meta['station'] = station['name']
    timeseries_meta['variable'] = 'WindGust'
    timeseries_meta['unit'] = 'm/s'
    timeseries_meta['type'] = station['type']
    timeseries_meta['source'] = station['source']
    timeseries_meta['name'] = station['run_name']

    print("#############Extracting and Pushing WindGust of Station: %s###############" % station['name'])

    _extract_n_push(
        extract_adapter,
        push_adapter,
        station,
        start_date,
        end_date,
        timeseries_meta,
        TimeseriesGroupOperation.mysql_5min_avg)


# TODO think of a normalization form.
def extract_n_push_winddirection(extract_adapter, push_adapter, station, start_date, end_date):
        # Create even metadata. Event metadata is used to create timeseries id (event_id) for the timeseries.
        timeseries_meta = copy.deepcopy(timeseries_meta_struct)
        timeseries_meta['station'] = station['name']
        timeseries_meta['variable'] = 'WindDirection'
        timeseries_meta['unit'] = 'degrees'
        timeseries_meta['type'] = station['type']
        timeseries_meta['source'] = station['source']
        timeseries_meta['name'] = station['run_name']

        print("#############Extracting and Pushing WindDirection of Station: %s###############" % station['name'])

        _extract_n_push(
            extract_adapter,
            push_adapter,
            station,
            start_date,
            end_date,
            timeseries_meta,
            TimeseriesGroupOperation.mysql_5min_avg)


def extract_n_push_solarradiation(extract_adapter, push_adapter, station, start_date, end_date):
    # Create even metadata. Event metadata is used to create timeseries id (event_id) for the timeseries.
    timeseries_meta = copy.deepcopy(timeseries_meta_struct)
    timeseries_meta['station'] = station['name']
    timeseries_meta['variable'] = 'SolarRadiation'
    timeseries_meta['unit'] = 'W/m2'
    timeseries_meta['type'] = station['type']
    timeseries_meta['source'] = station['source']
    timeseries_meta['name'] = station['run_name']

    print("#############Extracting and Pushing SolarRadiation of Station: %s###############" % station['name'])

    _extract_n_push(
        extract_adapter,
        push_adapter,
        station,
        start_date,
        end_date,
        timeseries_meta,
        TimeseriesGroupOperation.mysql_5min_avg)


def extract_n_push_humidity(extract_adapter, push_adapter, station, start_date, end_date):
        # Create even metadata. Event metadata is used to create timeseries id (event_id) for the timeseries.
        timeseries_meta = copy.deepcopy(timeseries_meta_struct)
        timeseries_meta['station'] = station['name']
        timeseries_meta['variable'] = 'Humidity'
        timeseries_meta['unit'] = '%'
        timeseries_meta['type'] = station['type']
        timeseries_meta['source'] = station['source']
        timeseries_meta['name'] = station['run_name']

        print("#############Extracting and Pushing Humidity of Station: %s###############" % station['name'])

        _extract_n_push(
            extract_adapter,
            push_adapter,
            station,
            start_date,
            end_date,
            timeseries_meta,
            TimeseriesGroupOperation.mysql_5min_avg)


def extract_n_push_waterlevel(extract_adapter, push_adapter, station, start_date, end_date):
    if 'mean_sea_level' not in station.keys():
        raise AttributeError('Attribute mean_sea_level is required.')
    msl = station['mean_sea_level']
    wl_min = station['min_wl']
    wl_max = station['max_wl']

    # Create even metadata. Event metadata is used to create timeseries id (event_id) for the timeseries.
    timeseries_meta = copy.deepcopy(timeseries_meta_struct)
    timeseries_meta['station'] = station['name']
    timeseries_meta['variable'] = 'Waterlevel'
    timeseries_meta['unit'] = 'm'
    timeseries_meta['type'] = station['type']
    timeseries_meta['source'] = station['source']
    timeseries_meta['name'] = station['run_name']

    print("#############Extracting and water level of Station: %s###############" % station['name'])

    _extract_n_push(
        extract_adapter,
        push_adapter,
        station,
        start_date,
        end_date,
        timeseries_meta,
        TimeseriesGroupOperation.mysql_5min_avg,
        timeseries_processor=_waterlevel_timeseries_processor, mean_sea_level=msl, waterLevel_min=wl_min, waterLevel_max=wl_max)
