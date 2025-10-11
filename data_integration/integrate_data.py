import requests
import numpy as np
import pandas as pd
import re
import sqlite3
import pathlib


def get_weather_data_from_api(url):
        response = requests.get(url)
        response_json = response.json()
        measurements_list = response_json['actual']['stationmeasurements']
        return measurements_list


def extract_measurement_data(data_list):
        measurements_extracted = [   {
                'timestamp': station.get('timestamp', np.nan),
                'temperature': station.get('temperature', np.nan),
                'groundtemperature': station.get('groundtemperature', np.nan),
                'feeltemperature': station.get('feeltemperature', np.nan),
                'windgusts': station.get('windgusts', np.nan),
                'windspeedBft': station.get('windspeedBft', np.nan),
                'humidity': station.get('humidity', np.nan),
                'precipitation': station.get('precipitation', np.nan),
                'sunpower': station.get('sunpower', np.nan),
                'stationid': station.get('stationid', np.nan)
            }
            for station in data_list
        ]
        measurements_df = pd.DataFrame(measurements_extracted)
        return measurements_df


def extract_station_data(data_list):
        stations_extracted = [   {
                'stationid': station.get('stationid', np.nan),
                'stationname': station.get('stationname', np.nan),
                'lat': station.get('lat', np.nan),
                'lon': station.get('lon', np.nan),
                'regio': station.get('regio', np.nan),
            }
            for station in data_list
        ]
        stations_df = pd.DataFrame(stations_extracted)
        return stations_df


def generate_measurement_id(row):
        station_id = row.get('stationid')
        timestamp = row.get('timestamp')
        numeric_parts = re.findall(r'\d+', str(timestamp))
        cleaned_timestamp = ''.join(numeric_parts)
        measurement_id = str(station_id) + cleaned_timestamp

        return measurement_id


def cast_measurements_types(df):
        dtype_mapping = {
                'temperature': 'float64',
                'groundtemperature': 'float64',
                'feeltemperature': 'float64',
                'windgusts': 'float64',
                'windspeedBft': 'float64',
                'humidity': 'float64',
                'precipitation': 'float64',
                'sunpower': 'float64',
                'stationid': 'int64',
                'measurementid': 'string'
        }
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        return df.astype(dtype_mapping)


def cast_stations_types(df):
        dtype_mapping = {
                'stationid': 'int64',
                'stationname': 'string',
                'lat': 'float64',
                'lon': 'float64',
                'regio': 'string'
        }
        return df.astype(dtype_mapping)


def write_data_to_sql(df_measurements, df_stations, db_name):
        script_path = pathlib.Path(__file__).resolve()
        root_dir = script_path.parent.parent
        data_dir = root_dir / 'data'
        data_dir.mkdir(parents=True, exist_ok=True)
        db_path = data_dir / db_name

        print(f'Connecting to database: {db_path}')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        try:
                cursor.execute('PRAGMA foreign_keys = ON;')
                conn.commit()

                stations_schema_sql = """
                        CREATE TABLE IF NOT EXISTS stations (
                            stationid INTEGER PRIMARY KEY,
                            station_name TEXT,
                            lat REAL,
                            lon REAL,
                            regio TEXT
                        );
                """
                cursor.execute(stations_schema_sql)
                print('Schema for "stations" created (stationid is PK/Index).')

                measurements_schema_sql = """
                        CREATE TABLE IF NOT EXISTS measurements (
                            measurementid TEXT PRIMARY KEY,
                            stationid INTEGER NOT NULL,
                            timestamp TEXT NOT NULL,
                            temperature REAL,
                            groundtemperature REAL,
                            feeltemperature REAL,
                            windgusts REAL,
                            windspeedBft REAL,
                            humidity REAL,
                            precipitation REAL,
                            sunpower REAL,
                            FOREIGN KEY(stationid) REFERENCES stations(stationid)
                        );
                """
                cursor.execute(measurements_schema_sql)
                print('Schema for "measurements" created (measurementid is PK/Index).')

                index_sql = "CREATE INDEX IF NOT EXISTS idx_measurements_stationid ON measurements (stationid);"
                cursor.execute(index_sql)
                print('Secondary index created on "measurements.stationid".')

                df_stations.to_sql('stations', conn, if_exists='replace', index=False) # TODO: change replace
                print("Data inserted into 'stations' table.")

                df_measurements.to_sql('measurements', conn, if_exists='append', index=False)
                print("Data inserted into 'measurements' table.")

                conn.commit()
                print("✅ Database saved successfully with defined keys and indexes.")

        except sqlite3.Error as e:
                print(f"Database error: {e}")

        finally:
                conn.close()


weather_data_url = 'https://data.buienradar.nl/2.0/feed/json'
measurements_list = get_weather_data_from_api(weather_data_url)

measurements_df = extract_measurement_data(measurements_list)
measurements_df['measurementid'] = measurements_df.apply(generate_measurement_id, axis=1)
measurements_df = cast_measurements_types(measurements_df)

stations_df = extract_station_data(measurements_list)
stations_df = cast_stations_types(stations_df)

write_data_to_sql(measurements_df, stations_df, 'weather_db.sqlite')
