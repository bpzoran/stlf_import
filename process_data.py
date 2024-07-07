import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text

import config

# Create a connection string for SQLAlchemy
connection_string = f"mssql+pyodbc://{config.server}/{config.database}?driver={config.driver}"

# Create an SQLAlchemy engine
engine = create_engine(connection_string)

# Establish connection to the database
conn = pyodbc.connect(
    f'DRIVER={config.driver};SERVER={config.server};DATABASE={config.database};Trusted_Connection=yes')
cursor = conn.cursor()

# Drop and create tables
cursor.execute(
    f"IF OBJECT_ID('{config.table_name_power_data_sumarized}', 'U') IS NOT NULL DROP TABLE {config.table_name_power_data_sumarized}")
cursor.execute(
    f"CREATE TABLE {config.table_name_power_data_sumarized} ([POWER_DATA_ID] INT IDENTITY(1,1) NOT NULL, [TIMEST] DATETIME NULL, [AKTIVNA_SNAGA] FLOAT NOT NULL)")
cursor.execute(
    f"IF OBJECT_ID('{config.table_name_power_data_skipped}', 'U') IS NOT NULL DROP TABLE {config.table_name_power_data_skipped}")
cursor.execute(f"CREATE TABLE {config.table_name_power_data_skipped} ([TIMEST] DATETIME NULL, [TS] VARCHAR(MAX) NULL)")
cursor.execute(
    f"IF OBJECT_ID('{config.table_name_power_data_simulated}', 'U') IS NOT NULL DROP TABLE {config.table_name_power_data_simulated}")
cursor.execute(f"CREATE TABLE {config.table_name_power_data_simulated} ([TIMEST] DATETIME NULL, [TS] VARCHAR(MAX) NULL, [FILE_NAME] VARCHAR(MAX) NULL, \
                [AKTIVNA_SNAGA_1] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_2] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_3] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_4] VARCHAR(MAX) NULL, \
                [AKTIVNA_SNAGA_5] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_6] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_7] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_8] VARCHAR(MAX) NULL, \
                [AKTIVNA_SNAGA_9] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_10] VARCHAR(MAX) NULL, [TIMEST_COPIED_FROM] DATETIME NULL)")

# Fetch data from PowerData table using SQLAlchemy engine
query = f"SELECT * FROM {config.table_name_power_data} order by TIMEST"
df = pd.read_sql(query, engine)

# Process data
unique_timestamps = list(df['TIMEST'].unique())
unique_timestamps.sort()
summarized_data = {}
skipped_rows = []
simulated_data = []
simulated_ts_timestamps = []
j = 0
for timestamp in unique_timestamps:
    if j % 240 == 0:
        print(f"processing timestamp {timestamp}")
    j += 1
    timestamp_data = df[df['TIMEST'] == timestamp]
    substations_missing_all = 0
    total_power = 0
    substations_with_data = 0
    skipped_rows_for_timest = []
    for _, row in timestamp_data.iterrows():

        substations_data_valid = False
        substation_power = 0

        for i in range(1, 11):
            power_value = row[f'AKTIVNA_SNAGA_{i}']
            try:
                power_value = float(power_value) if power_value is not None else 0
                if power_value > 0:
                    substation_power += power_value
                    substations_data_valid = True
            except ValueError:
                power_value = 0

        if substations_data_valid:
            total_power += substation_power
            substations_with_data += 1
        else:
            substations_missing_all += 1
            skipped_rows_for_timest.append((timestamp, row['TS']))

    if substations_missing_all <= 3:
        summarized_data[timestamp] = total_power
        if substations_missing_all > 0:
            simulated_ts_timestamps.extend(skipped_rows_for_timest)
    else:
        skipped_rows.extend([(timestamp, row['TS']) for _, row in timestamp_data.iterrows()])
for timst, ts in simulated_ts_timestamps:
    print(f"upgrade for timestamp {timst} and ts {ts}")
    with engine.connect() as connection:
        previous_data_sql = (text(f"SELECT TOP 1 * FROM {config.table_name_power_data} WHERE TIMEST < :timest and "
                                  f"datepart(weekday, TIMEST) = datepart(weekday, :timest) and "
                                  f"datepart(hour, TIMEST) = datepart(hour, :timest) and "
                                  f"TS = :ts and "
                                  f"("
                                  f"AKTIVNA_SNAGA_1 is not null or "
                                  f"AKTIVNA_SNAGA_2 is not null or "
                                  f"AKTIVNA_SNAGA_3 is not null or "
                                  f"AKTIVNA_SNAGA_4 is not null or "
                                  f"AKTIVNA_SNAGA_5 is not null or "
                                  f"AKTIVNA_SNAGA_6 is not null or "
                                  f"AKTIVNA_SNAGA_7 is not null or "
                                  f"AKTIVNA_SNAGA_8 is not null or "
                                  f"AKTIVNA_SNAGA_9 is not null or "
                                  f"AKTIVNA_SNAGA_10 is not null"
                                  f")"
                                  f"order by timest desc"))
        params = {"timest": timst.to_pydatetime(), "ts": ts}
        result = connection.execute(previous_data_sql, params)

        # Fetch all results
        rows = result.fetchall()
        if len(rows) > 0:
            simulated_data.append((timst, rows[0]))
        else:
            summarized_data = {key: val for key, val in summarized_data if key != timst}
            skipped_rows.append((timst, rows[0]))

unique_simulated_timestamps_set = set()
for timst, _ in simulated_data:
    unique_simulated_timestamps_set.add(timst)

unique_simulated_timestamps_list = list(unique_simulated_timestamps_set)

for timst in unique_simulated_timestamps_list:
    timstamp_ts_data = [row for (timest, row) in simulated_data if timest == timst]
    pload_sum = 0.0
    for row in timstamp_ts_data:
        simulated_power = 0.0
        for i in range(3, 13):
            power_value = row[i]
            try:
                power_value = float(power_value) if power_value is not None else 0
                if power_value > 0:
                    simulated_power += power_value
                    substations_data_valid = True
            except ValueError:
                power_value = 0
        pload_sum += simulated_power
    summarized_data[timst] += pload_sum

# Insert summarized data into PowerDataSumarized table
print("Insert summarized data into PowerDataSumarized table")
j = 0
for timest in summarized_data:
    akt_snaga = summarized_data[timest]
    if j % 240 == 0:
        print(f"summarized rows - insert timestamp {timest}")
    j += 1
    cursor.execute(f"INSERT INTO {config.table_name_power_data_sumarized} (TIMEST, AKTIVNA_SNAGA) VALUES (?, ?)",
                   timest, akt_snaga)

# Insert skipped rows into PowerDataSkipped table
print("Insert skipped rows into PowerDataSkipped table")
j = 0
for timest, ts in skipped_rows:
    if j % 240 == 0:
        print(f"skipped rows - insert timestamp {timest}")
    j += 1
    cursor.execute(f"INSERT INTO {config.table_name_power_data_skipped} (TIMEST, TS) VALUES (?, ?)", timest, ts)

# Insert simulated rows into PowerDataSimulated table
print("Insert skipped rows into PowerDataSkipped table")
j = 0
for timest, row in simulated_data:
    if j % 240 == 0:
        print(f"simulated rows - insert timestamp {timest}")
    j += 1
    cursor.execute(f"INSERT INTO PowerDataSimulated ([TIMEST] ,[TS] ,[FILE_NAME] ,[AKTIVNA_SNAGA_1] ,"
                   f"[AKTIVNA_SNAGA_2] ,[AKTIVNA_SNAGA_3] ,[AKTIVNA_SNAGA_4] ,[AKTIVNA_SNAGA_5] ,"
                   f"[AKTIVNA_SNAGA_6] ,[AKTIVNA_SNAGA_7] ,[AKTIVNA_SNAGA_8] ,[AKTIVNA_SNAGA_9] ,[AKTIVNA_SNAGA_10] ,"
                   f"[TIMEST_COPIED_FROM]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   row[0], row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12], timest.to_pydatetime())

# Commit transactions and close the connection
conn.commit()
cursor.close()
conn.close()
print("Process data finished")
