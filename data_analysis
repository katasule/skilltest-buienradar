import pandas as pd
import sqlite3
import pathlib
import datetime


def get_db_path(db_filename):
    script_path = pathlib.Path(__file__).resolve()
    data_dir = script_path.parent.parent / 'data'
    db_path = data_dir / db_filename

    return db_path


def run_sql_query(sql_query, db_filename):
    db_path = get_db_path(db_filename)

    if not db_path.exists():
        print(f'ERROR: Database file not found at path: {db_path}')
        return None

    print(f'Connecting to database: {db_path}')
    conn = sqlite3.connect(db_path)

    try:
        df_result = pd.read_sql_query(sql_query, conn)
        print("✅ Query executed successfully.")
        return df_result

    except sqlite3.Error as e:
        print(f'ERROR executing SQL query: {e}')
        return None

    finally:
        conn.close()


print("\n--- Running Query: Which weather station recorded the highest temperature? ---")
current_date = datetime.date.today().strftime('%Y-%m-%d')
query_1 = f"""
    SELECT
        temperature AS highest_temperature,
        stationname AS station
    FROM measurements
    LEFT JOIN stations ON measurements.stationid = stations.stationid
    WHERE CAST(STRFTIME('%Y-%m-%d', timestamp) AS TEXT) = '{current_date}'
    ORDER BY temperature DESC
    LIMIT 1;
"""
result_1 = run_sql_query(query_1, 'weather_db.sqlite')

if result_1 is not None:
    print(result_1)


print("\n--- Running Query: What is the average temperature? ---")
query_2 = """
    SELECT
        AVG(temperature) AS average_temperature
    FROM measurements
    WHERE timestamp = (SELECT MAX(timestamp) FROM measurements);
"""
result_2 = run_sql_query(query_2, 'weather_db.sqlite')

if result_2 is not None:
    print(result_2)


print("\n--- Running Query: What is the station with the biggest difference between feel temperature and the actual temperature? ---")
query_3 = """
    SELECT
        ABS(temperature - feeltemperature) AS temperature_difference,
        stationname AS station
    FROM measurements
    LEFT JOIN stations ON measurements.stationid = stations.stationid
    WHERE timestamp = (SELECT MAX(timestamp) FROM measurements)
    ORDER BY temperature_difference DESC
    LIMIT 1;
"""
result_3 = run_sql_query(query_3, 'weather_db.sqlite')

if result_3 is not None:
    print(result_3)


print("\n--- Running Query: Which weather station is located in the North Sea? ---")
query_4 = f"""
    SELECT
        stationname AS station
    FROM stations
    WHERE regio = 'Noordzee'
"""
result_4 = run_sql_query(query_4, 'weather_db.sqlite')

if result_4 is not None:
    print(result_4)
